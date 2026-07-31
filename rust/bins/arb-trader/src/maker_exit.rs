//! MAKER EXIT: resting an offer that flattens a basket, when the passive exit
//! pays against what we actually paid for it.
//!
//! `crate::unwind` selects. This places. Its module header is the spec this was
//! written against and it is not restated here; what IS restated is which of its
//! five obstacles this answers, which it answers by NARROWING THE TRADE rather
//! than by solving, and which it leaves open. Read that file first.
//!
//! # A MAKER EXIT IS NOT THE TAKE, AND THE TAKE STILL LOSES MONEY
//!
//! `unwind_apr` on the france lots is about −111%/yr: crossing the spread to get
//! out of them is a large realized loss and nothing here will ever do it. This
//! rests an ASK and only trades if somebody comes to us. Those are different
//! trades with different signs and the only reason this module can exist is that
//! they are different.
//!
//! # WHAT THIS DOES, EXACTLY
//!
//! One basket, one lot, one resting Kalshi ask, one PM-US close at fill:
//!
//!   1. `unwind::select` picks candidates against the hurdle in force;
//!   2. [`Debounce`] holds each one for [`DEBOUNCE_S`] of CONTINUOUS selection
//!      across at least [`DEBOUNCE_SCANS`] scans;
//!   3. the basis comes from `data/exec/trades.jsonl` — `naked_act::worst_lot`
//!      on BOTH legs, the dearest still-open lot of each;
//!   4. [`exit_limit`] solves for the lowest legal Kalshi ask at which selling
//!      that lot AND buying its PM-US YES back still locks [`MIN_LOCK`] a
//!      contract net of both legs' fees;
//!   5. the entry quoter is asked to yield the Kalshi ask and given
//!      [`SUPPRESS_SETTLE_S`] to do it;
//!   6. ONE post-only GTC ask rests, for at most [`MAX_CLIP`] contracts;
//!   7. on a fill the PM-US leg is closed with an IOC re-priced against the
//!      book AT THAT MOMENT, and one `unwound` record naming that lot's `ts`
//!      goes in through `ledger::append_basket`.
//!
//! # HOW §5's FIVE PROBLEMS ARE ANSWERED — THREE BY REFUSING TO HAVE THEM
//!
//!   * **AGGREGATE N BASKETS BEHIND ONE ASK, AND SPLIT A PARTIAL FILL BACK
//!     ACROSS N `closes_ts`.** NOT DONE, AND NOT NEEDED, because the trade was
//!     narrowed until the problem stopped existing. `worst_lot` already answers
//!     "which lot" with the DEAREST one, and the exit is sized to THAT LOT
//!     ALONE. One exit therefore names exactly one `closes_ts` and no split can
//!     arise. The cost is that a ticker carrying six lots takes six passes to
//!     empty rather than one; the benefit is that the hardest correctness
//!     requirement in that header is structurally absent rather than
//!     implemented. Pricing against the dearest lot is also the only
//!     attribution-safe choice — contracts are fungible, and an exit that pays
//!     against the dearest pays against every other one — which is
//!     `worst_lot`'s own argument, in the other direction.
//!   * **RECORD THAT AN EXIT IS OUTSTANDING.** [`Resting`], and it is the reason
//!     `select` re-choosing the same basket next scan cannot rest a second ask:
//!     [`Live::target`] refuses while anything is outstanding at all. The cap is
//!     [`MAX_RESTING`] = 1 across the whole process, so "a second ask" is not a
//!     bug this can express.
//!   * **PULL THE ENTRY QUOTE FIRST.** [`request_suppress`] publishes the (market,
//!     Ask) pair; `Engine::maker_exit_tick` merges it into every quoter's
//!     suppress set and publishes back WHEN it did. Nothing is placed until that
//!     has held for [`SUPPRESS_SETTLE_S`]. **THIS IS A BOUNDED WAIT AND NOT A
//!     PROOF** — nothing here reads the venue's resting list — and the reason
//!     that is acceptable is that the exit is POST-ONLY. Kalshi's self-trade
//!     prevention answers a post-only collision by REJECTING the order, which
//!     costs a log line and a cycle. The 14 deadlocked unwinds of card ed6a5910
//!     were IOCs, where the same collision eats the whole clip.
//!   * **RE-PRICE AT FILL TIME, AGAINST THE BOOK, AND ABANDON IF IT NO LONGER
//!     PAYS.** [`close_limit`] does the re-price and [`Live::on_fill`] does the
//!     abandon — but read what "abandon" means here, because it is NOT the same
//!     as declining to trade. By the time this runs the Kalshi ask HAS FILLED:
//!     we are already flat one leg and long the other. Refusing to close leaves
//!     a naked PM-US short, which is a real directional position. So the refusal
//!     is loud, lands on a must-stay-0 gauge, and is left to the naked-leg
//!     reconciliation — see the last section.
//!   * **§1, THE BLOCKER, IS HALF RETRACTED.** Re-derived on 2026-07-31 rather
//!     than inherited:
//!       - the half that BLOCKED is gone. That header was written when the armed
//!         process ran three families; it now runs `--rel-prefix xvus-`, and all
//!         25 priced positions in `data/exec/marks.json` are `xvus-`. The
//!         candidate set and the owned set are no longer disjoint — they are
//!         identical. [`Live::target`] still refuses anything `unwind` marked
//!         unactionable, so the test is enforced rather than assumed.
//!       - the half that REMAINS is the coverage gap, and it is unchanged. All 6
//!         `source: arb-trader` open baskets in the live ledger carry no
//!         `cost_usd`, so `mark_positions.py:280-282` skips them and they are
//!         absent from `marks.json` (checked 2026-07-31: 6 of 32 open baskets,
//!         plus one `mltox-` probe basket). **THIS MODULE CANNOT EXIT A BASKET
//!         THIS ENGINE OPENED.** That is a coverage limit, not a hazard: a
//!         basket invisible to the input is never selected, and a ticker whose
//!         qty is under-counted rests a SMALLER ask than we could — the safe
//!         direction. It becomes a hazard only if someone reads
//!         `unwind_candidates` as "the book".
//!
//! # THE DEBOUNCE IS SIZED ON THE TAPE, NOT ON A GUESS
//!
//! See [`DEBOUNCE_S`]. The short version: on 4,735 samples of
//! `data/exec/marks_history.jsonl` spanning 172.7 h, 60% of every excursion
//! above the two-tick floor is ONE SAMPLE LONG.
//!
//! # WHAT THIS STILL CANNOT DO
//!
//! It cannot make the fill-time PM close safe, only bounded. Between the Kalshi
//! ask filling and the PM-US IOC returning we are one-legged, and if the IOC
//! fails or is refused we STAY one-legged. Two things bound that and neither
//! removes it: [`MAX_CLIP`] is 5 contracts, and `--positions-recon-act` already
//! runs in this same binary every 5 minutes and completes profitable naked legs
//! from venue truth. **THOSE TWO WILL FIGHT.** The naked leg this leaves is a
//! `PmShort` finding, and `naked_act` answers a `PmShort` by BUYING Kalshi YES —
//! i.e. by re-opening the basket this just exited. It will only do so
//! profitably, against its own ledger basis, so the money is safe; what is not
//! safe is an operator's model of what the process is doing. If both are armed,
//! `positions_recon_acted` climbing in step with `maker_exit_unresolved` is that
//! fight, and it is a reason to disarm one of them, not a curiosity.
//!
//! It also prices the PM close off a book that INCLUDES OUR OWN RESTING SIZE.
//! `unwind::MIN_EXIT_CT` names this bias exactly — always adverse, a tick is a
//! lower bound on it and not a bound — and the answer here is the same
//! instalment: [`close_limit`] pays one full [`TICK`] THROUGH the ask it can
//! see. On a book where our own quote is the only ask that is not enough, and
//! nothing in this process can tell us when that is true.

use crate::ledger;
use crate::naked_act::{ceil_to_tick, worst_lot, Held};
pub use crate::naked_act::MIN_LOCK;
use arb_core::fees::{FeeSchedule, Role};
use arb_core::clock::now_s as wall_now;
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use arb_venue::gateway::Quote;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrd};
use std::sync::Mutex;
use std::time::{Duration, Instant};

// ------------------------------------------------------------------ policy ---

/// How long a candidate must be CONTINUOUSLY selected before an ask may rest on
/// it. Fifteen minutes.
///
/// SIZED ON THE TAPE THE HEADER POINTS AT, re-measured on the current file
/// rather than inherited. `data/exec/marks_history.jsonl` on 2026-07-31:
/// 4,735 samples over 172.7 h, median writer period 120.4 s, 29 tracked baskets.
/// Against `unwind::MIN_EXIT_CT` (two ticks):
///
/// ```text
///   13 of 29 baskets cross the floor at least once
///   the worst crosses it 111 times — one every 93 minutes
///   202 separate excursions ABOVE the floor, of which
///     122 (60%) are a SINGLE SAMPLE — they are gone by the next write
///      67 (33%) survive 300 s
///      55 (27%) survive 900 s
/// ```
///
/// So the median excursion above the floor has NO DURATION AT ALL, and that is
/// what the debounce is for: 900 s discards 147 of the 202 excursions — every
/// single-sample spike and most of the rest — and admits the 27% that persist.
/// It is deliberately NOT sized to admit "most" candidates: an exit that is
/// still there a quarter of an hour later is a different fact from a print, and
/// the whole failure mode being avoided is resting an ask against a number that
/// existed for one 120-second window.
///
/// It is also `taketake::MAX_MARKS_AGE_S` exactly, which is not a coincidence
/// worth hiding: a candidate must outlive one whole staleness window of the file
/// it came from before it is allowed to cost money.
///
/// **IT IS NOT A CLAIM THAT THE 27% ARE DURABLE.** `unwind::MIN_EXIT_CT` records
/// a basket going +0.0312 -> −0.0721 in ONE writer period. Nothing sized in
/// wall-clock can survive that; what the debounce buys is that the 60% which
/// never had a second sample never reach a venue.
pub const DEBOUNCE_S: f64 = 900.0;

/// ...and across at least this many DISTINCT scans.
///
/// Three. Time alone is satisfiable by one observation and a slow loop: if the
/// exit cycle stalls for twenty minutes and comes back, a candidate seen once
/// before the stall and once after has been "continuously selected for 900 s" on
/// the clock and observed twice in fact. `unwind::select` refuses marks older
/// than 900 s, so neither sample can be ancient — but two samples of a
/// two-sample tape is exactly the noise this is filtering.
pub const DEBOUNCE_SCANS: u32 = 3;

/// Contracts in ONE resting exit.
///
/// Five, and the same reasoning as `naked_act::MAX_CLIP`, which this
/// deliberately matches: everything here is priced off a basis RECONSTRUCTED
/// from ledger records rather than off a fill we watched, and that
/// reconstruction has never met live money. Five contracts bounds the cost of
/// the reconstruction being wrong at a few dollars. A 34-lot france basket still
/// empties — in seven passes, each re-decided from scratch against a fresh book.
pub const MAX_CLIP: i64 = 5;

/// Exits resting at once, across the whole process. ONE.
///
/// This is the answer to `unwind` §5's "a naive placer rests a SECOND ask", and
/// it is a cap rather than a per-market interlock on purpose: a per-market rule
/// would still let one pass rest five asks on five markets, five separate
/// one-legged exposures, each of which becomes a naked PM-US short the moment it
/// fills. One at a time means at most one leg can be in flight, ever.
pub const MAX_RESTING: usize = 1;

/// How long the entry quoter is given to yield the Kalshi ask before an exit is
/// rested on it.
///
/// 30 s: the suppress set is installed on the 60 s `apr_tick`, so this is one
/// tick plus a cancel round trip. See the module header for why a bounded wait
/// rather than a proof is tolerable HERE and was not tolerable for the Python's
/// IOC.
pub const SUPPRESS_SETTLE_S: f64 = 30.0;

/// The price increment both legs quote in, for the tick THROUGH the PM ask.
/// `mark_positions.py` steps PM in `Decimal("0.01")`; this is the resolution of
/// the venue, not a modelling choice.
const TICK: &str = "0.01";

/// How stale the engine's published view may be. Three stats ticks.
///
/// FAIL-CLOSED on both "never published" and "too old", for the same reason
/// `naked_act::inflight_check` is: the view carries the hurdle, the cap and the
/// PM book, and the honest answer to "what is the hurdle" from a silent engine
/// is not a number. It is also how a KILLED engine stops this module — the
/// publisher does not run while `killed` or `feed_reason` is set, so the view
/// ages out and every decision here refuses.
const VIEW_MAX_AGE: Duration = Duration::from_secs(180);

/// Ticks the limit search walks before giving up. `naked_act::MAX_TICK_WALK`'s
/// reasoning, and its number.
const MAX_TICK_WALK: u32 = 40;

// ------------------------------------------------------------------ gauges ---

/// Exit asks this process has RESTED at a venue (0 forever without the flag).
static PLACED: AtomicU64 = AtomicU64::new(0);
/// Exits that FILLED and whose basket was booked closed.
static CLOSED: AtomicU64 = AtomicU64::new(0);
/// Decisions declined. NOT an error count — declining is the resting state.
static REFUSED: AtomicU64 = AtomicU64::new(0);
/// **MUST STAY 0.** A Kalshi exit ask FILLED and its PM-US leg did not close.
/// The account is one-legged and the ledger still says the basket is open, which
/// is worse than either half alone: the exposure fold reads a hedged basket that
/// is not hedged. Never folded into [`REFUSED`] — "the book did not pay" and
/// "we are naked and our books disagree" must never share a number.
static UNRESOLVED: AtomicU64 = AtomicU64::new(0);

pub fn placed() -> u64 {
    PLACED.load(AtomicOrd::Relaxed)
}
pub fn closed() -> u64 {
    CLOSED.load(AtomicOrd::Relaxed)
}
pub fn refused() -> u64 {
    REFUSED.load(AtomicOrd::Relaxed)
}
pub fn unresolved() -> u64 {
    UNRESOLVED.load(AtomicOrd::Relaxed)
}

fn refuse(why: String) -> String {
    REFUSED.fetch_add(1, AtomicOrd::Relaxed);
    why
}

// ------------------------------------------------ the engine's published view ---

/// What the ENGINE knows and this module cannot derive: the hurdle in force, the
/// cap it was derived from, the PM-US book, and which Kalshi asks it has yielded.
///
/// PUBLISHED WHOLESALE once per tick rather than maintained incrementally, for
/// `naked_act::Inflight`'s reason: a missed incremental update to `suppressed`
/// would be an exit resting against our own quote, and overwriting the whole
/// value from the one place that owns it cannot express that mistake.
#[derive(Debug, Clone, Default)]
pub struct EngineView {
    /// `Engine::apr_bar` — the number actually installed on the quoters, not a
    /// second derivation of it (`unwind` §4).
    pub apr_bar: f64,
    /// `RiskView::global_cap_usd`. `unwind::select` refuses a degenerate one.
    pub global_cap_usd: f64,
    /// PM-US market -> best YES ASK, the price a close would BUY through.
    /// Absent means the engine holds no book or the side is empty; either way
    /// the close is unpriceable and the exit is refused.
    pub pm_ask: BTreeMap<String, String>,
    /// Kalshi markets whose ASK the quoters have been told to yield, and the
    /// instant that was installed. A market absent here has NOT been yielded.
    pub suppressed_at: BTreeMap<String, Instant>,
}

struct Published {
    view: EngineView,
    at: Instant,
}

static VIEW: Mutex<Option<Published>> = Mutex::new(None);
static SUPPRESS_REQ: Mutex<Option<BTreeSet<String>>> = Mutex::new(None);

/// Publish the engine's view. Called from `Engine::maker_exit_tick` and nowhere
/// else.
pub fn publish_view(view: EngineView) {
    if let Ok(mut g) = VIEW.lock() {
        *g = Some(Published { view, at: Instant::now() });
    }
}

/// The engine's view, or why it may not be believed.
pub fn engine_view() -> Result<EngineView, String> {
    let g = VIEW.lock().map_err(|_| "the engine view registry is poisoned".to_string())?;
    let Some(p) = g.as_ref() else {
        return Err(
            "the engine has never published its hurdle, cap and PM-US book — there is no \
             number here to decide an exit against, and one will not be invented"
                .into(),
        );
    };
    let age = p.at.elapsed();
    if age > VIEW_MAX_AGE {
        return Err(format!(
            "the engine's view is {:.0}s old (it publishes on the 60s tick, and stops \
             publishing when killed or feed-pulled) — the hurdle, the cap and the PM-US book \
             are all unknown",
            age.as_secs_f64()
        ));
    }
    Ok(p.view.clone())
}

/// Ask the entry quoters to yield these Kalshi asks. Read by
/// `Engine::maker_exit_tick`.
pub fn request_suppress(markets: BTreeSet<String>) {
    if let Ok(mut g) = SUPPRESS_REQ.lock() {
        *g = Some(markets);
    }
}

/// The outstanding suppression request, for the engine.
pub fn suppress_requests() -> BTreeSet<String> {
    SUPPRESS_REQ.lock().ok().and_then(|g| g.clone()).unwrap_or_default()
}

/// [`VIEW`] and [`SUPPRESS_REQ`] are process-wide and `cargo test` runs every
/// test in one process, so a test that resets them can blank the very view
/// another one is asserting on. It did: the suppress-install test read "never
/// published" on its own publication. Every test in ANY module that touches
/// these takes this first — which is why it lives here rather than inside
/// `mod tests`.
///
/// SYNC, unlike `naked_act::TEST_SERIAL`: these are sync tests and a
/// `tokio::sync::Mutex` cannot be awaited from one. Poison-recovering, because a
/// panicking test must not cascade into every other test in the group.
#[cfg(test)]
pub(crate) static VIEW_TEST_SERIAL: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn test_serial() -> std::sync::MutexGuard<'static, ()> {
    VIEW_TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
pub(crate) fn reset_view() {
    if let Ok(mut g) = VIEW.lock() {
        *g = None;
    }
    if let Ok(mut g) = SUPPRESS_REQ.lock() {
        *g = None;
    }
}

// --------------------------------------------------------------- the debounce ---

/// How long each candidate has been CONTINUOUSLY selected, and over how many
/// scans.
///
/// Keyed on `(rel_id, opened_ts.to_bits())` — `unwind::identity_set`'s key, and
/// the basket identity `closes_ts` addresses. Keyed on the relationship alone it
/// would credit one lot's persistence to another lot on the same relationship,
/// which is precisely the six-lot france shape.
///
/// A candidate that DISAPPEARS from a scan is forgotten outright rather than
/// decayed. That is what makes this a debounce and not a moving average: the
/// signal's failure mode is a one-sample spike, and anything that remembers a
/// spike across its own absence re-admits it.
#[derive(Debug, Default)]
pub struct Debounce {
    seen: BTreeMap<(String, u64), (f64, u32)>,
}

impl Debounce {
    /// Fold one scan in and return the candidates that have now held long
    /// enough, in the order they were given.
    pub fn admit<'a>(
        &mut self,
        exits: &'a [crate::unwind::Exit],
        now: f64,
    ) -> Vec<&'a crate::unwind::Exit> {
        let mut next: BTreeMap<(String, u64), (f64, u32)> = BTreeMap::new();
        let mut out = Vec::new();
        for e in exits {
            let k = (e.rel_id.clone(), e.opened_ts.to_bits());
            let (first, scans) = match self.seen.get(&k) {
                Some((f, n)) => (*f, n + 1),
                None => (now, 1),
            };
            next.insert(k, (first, scans));
            if now - first >= DEBOUNCE_S && scans >= DEBOUNCE_SCANS {
                out.push(e);
            }
        }
        self.seen = next;
        out
    }

    /// How long this basket has been held, for the log line. `None` = not seen.
    pub fn held_s(&self, e: &crate::unwind::Exit, now: f64) -> Option<f64> {
        self.seen.get(&(e.rel_id.clone(), e.opened_ts.to_bits())).map(|(f, _)| now - f)
    }
}

// ---------------------------------------------------------------- the prices ---

/// The lowest legal Kalshi ask at which selling this lot AND buying its PM-US
/// YES back still locks [`MIN_LOCK`] a contract, net of both legs' fees.
///
/// Per contract, with everything an all-in number:
///
/// ```text
///   proceeds   L − maker_fee(L)          the Kalshi YES we sell
///            + (1 − pm_close) − taker_fee(pm_close)
///                                        the PM-US NO we sell, i.e. buying
///                                        the YES back at pm_close
///   paid       k_basis + pm_basis        what the ledger says the lot cost
///   require    proceeds − paid >= MIN_LOCK
/// ```
///
/// Solved by walking UP the ladder from the fee-free bound, for `sell_limit`'s
/// reasons: there is no closed form over a tapered ladder, `p − fee(p)` is
/// strictly increasing so the walk terminates at the first solution, and a
/// search that only moves in the safe direction cannot overshoot into a price we
/// would not have accepted.
///
/// THE FEE ON THE KALSHI LEG IS THE **MAKER** SCHEDULE, and that is the one
/// place this deliberately differs from `naked_act::sell_limit`. This order
/// RESTS; it cannot fill as a taker, because it is post-only and a post-only
/// that would cross is rejected rather than crossed. Charging the taker fee
/// would price an order that cannot happen — and in the expensive direction, so
/// it would silently select nothing on thin books.
#[allow(clippy::too_many_arguments)]
pub fn exit_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    ladder: &[(String, String, String)],
    k_basis: D,
    pm_basis: D,
    pm_close: D,
    qty: i64,
) -> Result<D, String> {
    let lock = cx.parse_exact(MIN_LOCK);
    // What the PM leg gives back, net of the fee to take it.
    let size = cx.from_i64(qty);
    let pm_fee_total = fees.fee(cx, Venue::PolymarketUs, Role::Taker, pm_close, size, "");
    let pm_fee = cx.div(pm_fee_total, size);
    let pm_out = cx.one_minus(pm_close);
    let pm_out = cx.sub(pm_out, pm_fee);
    // L must cover: what we paid, less what PM gives back, plus the lock, plus
    // the Kalshi maker fee at L.
    let paid = cx.add(k_basis, pm_basis);
    let want = cx.add(paid, lock);
    let want = cx.sub(want, pm_out);
    if !cx.is_pos(want) {
        // The PM leg alone already returns more than the lot cost plus the lock,
        // so any positive Kalshi ask pays. It has to be POSITIVE: the bottom of
        // a penny ladder is $0.0000, and an ask at zero is not a price — it is
        // an offer to give the contracts away, which the venue would either
        // reject or fill instantly against the first bid. Start at the smallest
        // legal tick above zero.
        let zero = cx.zero();
        let Some(bottom) = ceil_to_tick(cx, ladder, zero) else {
            return Err("the tick ladder has no bottom rung".into());
        };
        if cx.is_pos(bottom) {
            return Ok(bottom);
        }
        let (_, step) = rung_step(cx, ladder, bottom)?;
        let up = cx.add(bottom, step);
        return ceil_to_tick(cx, ladder, up)
            .ok_or_else(|| "the tick ladder has no positive price".to_string());
    }
    // A required ask of a dollar or more is the answer, not a ladder problem.
    // Checked BEFORE the spelling, because `ceil_to_tick` answers an off-ladder
    // price with "no legal spelling" — true, useless, and the commonest way this
    // fails: it is what an operator sees for every lot that has moved against
    // us, and it says nothing about why.
    if cx.cmp(want, cx.one) != Ordering::Less {
        return Err(format!(
            "exiting this lot profitably would need a Kalshi ask of {} — at or above $1, \
             against a basis of {} + {} with the PM-US close at {}. It cannot be done: the \
             pair pays out at most $1 and this lot has moved against us.",
            cx.emit_6dp(want),
            cx.emit_6dp(k_basis),
            cx.emit_6dp(pm_basis),
            cx.emit_6dp(pm_close)
        ));
    }
    let Some(mut l) = ceil_to_tick(cx, ladder, want) else {
        return Err(format!(
            "{} is outside every rung of the venue's tick ladder, so it has no legal spelling",
            cx.emit_6dp(want)
        ));
    };
    for _ in 0..MAX_TICK_WALK {
        let k_fee_total = fees.fee(cx, Venue::Kalshi, Role::Maker, l, size, "");
        let k_fee = cx.div(k_fee_total, size);
        let net = cx.sub(l, k_fee);
        if cx.cmp(net, want) != Ordering::Less {
            return Ok(l);
        }
        let (_, step) = rung_step(cx, ladder, l)?;
        let higher = cx.add(l, step);
        let Some(next) = ceil_to_tick(cx, ladder, higher) else {
            return Err(format!(
                "no price on the ladder nets {} after the Kalshi maker fee — this lot cannot \
                 be exited at a profit at any legal price",
                cx.emit_6dp(want)
            ));
        };
        l = next;
        if cx.cmp(l, cx.one) != Ordering::Less {
            return Err(format!(
                "exiting this lot profitably would need a Kalshi ask at or above $1 against a \
                 basis of {} + {} — it cannot be done",
                cx.emit_6dp(k_basis),
                cx.emit_6dp(pm_basis)
            ));
        }
    }
    Err("the exit limit search did not converge — refusing rather than guessing".into())
}

/// The rung step at `x`, as an error rather than an `Option` so the walk above
/// reads in one direction.
fn rung_step(
    cx: &mut Cx,
    ladder: &[(String, String, String)],
    x: D,
) -> Result<(D, D), String> {
    for (s, e, st) in ladder {
        let (Some(s), Some(e), Some(st)) = (cx.parse(s), cx.parse(e), cx.parse(st)) else {
            return Err("the venue's tick ladder does not parse".into());
        };
        if cx.cmp(x, s) != Ordering::Less && cx.cmp(x, e) == Ordering::Less {
            return Ok((s, st));
        }
    }
    Err(format!("{} is outside every rung of the tick ladder", cx.emit_6dp(x)))
}

/// The price a PM-US close may pay AT MOST, given what the Kalshi leg actually
/// sold for. The fill-time re-price (`unwind` §5's fourth bullet).
///
/// This is [`exit_limit`] solved for the other unknown: the Kalshi side is now a
/// FACT (`k_fill`, what the venue gave us), so the question is how dear the PM
/// YES may be and still leave [`MIN_LOCK`] locked.
///
/// ```text
///   k_fill − maker_fee(k_fill) + (1 − p) − taker_fee(p) − k_basis − pm_basis >= MIN_LOCK
/// ```
///
/// `p + fee(p)` is strictly increasing, so the largest `p` satisfying it is
/// found by walking DOWN from the fee-free bound. A close that cannot be made at
/// or under this price is REFUSED — and refusing here means staying naked, which
/// is why the caller alarms rather than shrugs.
pub fn close_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    k_fill: D,
    k_basis: D,
    pm_basis: D,
    qty: i64,
) -> Result<D, String> {
    let lock = cx.parse_exact(MIN_LOCK);
    let size = cx.from_i64(qty);
    let k_fee_total = fees.fee(cx, Venue::Kalshi, Role::Maker, k_fill, size, "");
    let k_fee = cx.div(k_fee_total, size);
    let k_out = cx.sub(k_fill, k_fee);
    // 1 − p − fee(p) >= MIN_LOCK + k_basis + pm_basis − k_out
    let need = cx.add(lock, k_basis);
    let need = cx.add(need, pm_basis);
    let need = cx.sub(need, k_out);
    // so p + fee(p) <= 1 − need
    let ceiling = cx.one_minus(need);
    if !cx.is_pos(ceiling) {
        return Err(format!(
            "the Kalshi leg returned {} against a lot that cost {} + {}, so no PM-US price \
             above zero closes this basket with {MIN_LOCK} locked",
            cx.emit_6dp(k_out),
            cx.emit_6dp(k_basis),
            cx.emit_6dp(pm_basis)
        ));
    }
    // PM-US quotes cents; walk down in cents from the fee-free bound.
    let tick = cx.parse_exact(TICK);
    let mut p = quantize_down(cx, ceiling, tick);
    for _ in 0..MAX_TICK_WALK {
        if !cx.is_pos(p) {
            return Err(format!(
                "no positive PM-US price closes this basket with {MIN_LOCK} locked against a \
                 Kalshi fill of {}",
                cx.emit_6dp(k_fill)
            ));
        }
        let fee_total = fees.fee(cx, Venue::PolymarketUs, Role::Taker, p, size, "");
        let fee = cx.div(fee_total, size);
        let all_in = cx.add(p, fee);
        if cx.cmp(all_in, ceiling) != Ordering::Greater {
            return Ok(p);
        }
        p = cx.sub(p, tick);
    }
    Err("the close limit search did not converge — refusing rather than guessing".into())
}

/// The largest multiple of `tick` at or below `x`. PM-US has one flat cent
/// ladder, so there is no rung to be relative to.
fn quantize_down(cx: &mut Cx, x: D, tick: D) -> D {
    let n = cx.div(x, tick);
    let n = cx.quantize_int_down(n);
    cx.mul(n, tick)
}

// -------------------------------------------------------------- the decision ---

/// One exit, priced and ready to rest.
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub rel_id: String,
    /// The Kalshi ticker the ask rests on.
    pub market: String,
    /// The PM-US market whose NO is closed when it fills.
    pub pm_market: String,
    pub qty: i64,
    /// 4dp, the way the wire wants it.
    pub limit: String,
    /// The `ts` of the open record this closes — ONE record, by construction.
    /// See the module header on why no split is needed.
    pub closes_ts: f64,
    pub k_basis: String,
    pub pm_basis: String,
    /// The PM-US ask the exit was PRICED against. Not the price it will close
    /// at — that is re-derived at fill time by [`close_limit`] — but the number
    /// the decision rested on, so the log line and the record both name it.
    pub pm_ask_at_decision: String,
}

/// Decide the exit for one debounced candidate. Pure.
///
/// Every refusal is a `String` a human can act on: at 3am "the book is not
/// paying" and "we hold a lot our own ledger has never heard of" are different
/// messages and only one of them needs anybody.
#[allow(clippy::too_many_arguments)]
pub fn decide(
    cx: &mut Cx,
    fees: &FeeSchedule,
    records: &[Value],
    cand: &crate::unwind::Exit,
    pm_market: &str,
    quote: &Quote,
    view: &EngineView,
    now: Instant,
) -> Result<Order, String> {
    if !cand.actionable {
        return Err(refuse(format!(
            "{} is outside this process's --rel-prefix scope — it is not ours to exit \
             (unwind §1)",
            cand.rel_id
        )));
    }
    if quote.status != "active" {
        return Err(refuse(format!(
            "Kalshi reports {} as `{}`, not `active` — a market that is not trading takes no \
             order, whatever the marks file says. This is the guard `unwind::Skip::NotPriceable` \
             cannot be: a priced row in a FROZEN marks file is not evidence of a live book.",
            cand.market_id, quote.status
        )));
    }
    match view.suppressed_at.get(&cand.market_id) {
        None => {
            return Err(refuse(format!(
                "the entry quoter has not yet been told to yield {}:ask — asked for it this \
                 cycle; nothing rests until it has (unwind §5, card ed6a5910)",
                cand.market_id
            )))
        }
        Some(since) if now.saturating_duration_since(*since).as_secs_f64() < SUPPRESS_SETTLE_S => {
            return Err(refuse(format!(
                "{}:ask was yielded {:.0}s ago; giving it {SUPPRESS_SETTLE_S:.0}s to cancel \
                 before resting against it",
                cand.market_id,
                now.saturating_duration_since(*since).as_secs_f64()
            )))
        }
        Some(_) => {}
    }
    let Some(pm_ask) = view.pm_ask.get(pm_market) else {
        return Err(refuse(format!(
            "no PM-US ask for {pm_market} in the engine's book — the close leg is unpriceable, \
             so the exit is unpriceable. An exit whose second leg cannot be valued is a \
             one-legged trade with a number on it."
        )));
    };
    let Some(pm_ask_d) = cx.parse(pm_ask) else {
        return Err(refuse(format!("{pm_market} quoted an unparseable ask {pm_ask:?}")));
    };
    // THE BASIS, BOTH LEGS, FROM OUR OWN LEDGER. Never PM-US `costPerShare`,
    // which is `baseCost/|net|` — a residual that blows up as net -> 0, which is
    // exactly the regime an exit reads it in. `naked_act` carries the incident.
    let k_lot = worst_lot(cx, fees, records, &cand.rel_id, Venue::Kalshi, &cand.market_id, Held::LongYes)
        .map_err(|e| refuse(format!("Kalshi leg: {e}")))?;
    let pm_lot =
        worst_lot(cx, fees, records, &cand.rel_id, Venue::PolymarketUs, pm_market, Held::ShortYes)
            .map_err(|e| refuse(format!("PM-US leg: {e}")))?;
    let qty = k_lot.qty.min(pm_lot.qty).min(cand.qty).min(MAX_CLIP);
    if qty < 1 {
        return Err(refuse(format!(
            "nothing to exit on {}: kalshi lot {}, pm lot {}, marks {}",
            cand.rel_id, k_lot.qty, pm_lot.qty, cand.qty
        )));
    }
    let k_basis = cx
        .parse(&k_lot.cost_per_ct)
        .ok_or_else(|| refuse(format!("unparseable Kalshi basis {}", k_lot.cost_per_ct)))?;
    let pm_basis = cx
        .parse(&pm_lot.cost_per_ct)
        .ok_or_else(|| refuse(format!("unparseable PM-US basis {}", pm_lot.cost_per_ct)))?;
    // ONE TICK THROUGH THE ASK, matching `mark_positions.py`'s `p_ask + 0.01`.
    // It is not a bound on the self-pricing bias — see the module header — it is
    // the smallest instalment the grid can express on it.
    let tick = cx.parse_exact(TICK);
    let pm_close = cx.add(pm_ask_d, tick);
    let limit = exit_limit(cx, fees, &quote.ladder, k_basis, pm_basis, pm_close, qty)
        .map_err(refuse)?;
    // A resting ASK at or below the best BID is not a maker order, it is a take
    // dressed as one: post-only would reject it, and if it did not it would
    // cross into the book at a price this module never decided to take. Refuse.
    if let Some(bid) = quote.yes_bid.as_deref().and_then(|b| cx.parse(b)) {
        if cx.cmp(limit, bid) != Ordering::Greater {
            return Err(refuse(format!(
                "the profit floor for {} is {} and the book is bid {bid_s} — resting there \
                 would CROSS, which is the take this module exists not to do (unwind_apr on \
                 these lots is deeply negative). Waiting for a book that comes to us.",
                cand.market_id,
                cx.emit_6dp(limit),
                bid_s = quote.yes_bid.as_deref().unwrap_or("?"),
            )));
        }
    }
    let limit = cx.quantize_4dp(limit);
    Ok(Order {
        rel_id: cand.rel_id.clone(),
        market: cand.market_id.clone(),
        pm_market: pm_market.to_string(),
        qty,
        limit: limit.to_standard_notation_string(),
        closes_ts: k_lot.open_ts,
        k_basis: k_lot.cost_per_ct,
        pm_basis: pm_lot.cost_per_ct,
        pm_ask_at_decision: pm_ask.clone(),
    })
}

// ----------------------------------------------------------------- the record ---

/// The ledger record a filled-and-closed exit writes: a partial unwind of the
/// ONE lot it was priced against.
///
/// `closes_ts` is `k_lot.open_ts` and not a list, because the exit was sized to
/// that lot alone. That is the whole of `unwind` §5's split problem, answered by
/// not having it.
///
/// It does NOT claim a realized P&L. Both legs traded here — unlike
/// `naked_act::close_record`, where only one did — but the venue FEES arrive on
/// fill reports this process does not read, so `fees_pending` says what
/// `engine::fill::book_basket` says and for the same reason. The prices are the
/// ones we actually got.
pub fn close_record(o: &Order, k_fill: &str, pm_fill: &str, filled: i64, ts: f64) -> Value {
    serde_json::json!({
        "ts": ts,
        "relationship_id": o.rel_id,
        "title": format!("{} (rust maker-exit)", o.rel_id),
        "strategy": "maker-exit",
        "status": "unwound",
        "closes_ts": o.closes_ts,
        "qty": filled,
        "source": ledger::SOURCE,
        "fees_pending": true,
        "maker_exit_k_basis": o.k_basis,
        "maker_exit_pm_basis": o.pm_basis,
        "note": "opportunistic maker exit: a post-only Kalshi ask rested at the price that \
                 locked a profit against BOTH legs' ledger basis, and the PM-US NO was closed \
                 with an IOC re-priced against the book at fill time. Sized to ONE open lot, \
                 so this closes exactly one record. realized_pnl_usd is absent because the \
                 venue fees arrive on fill reports this process does not read.",
        "legs": [
            {"venue": "kalshi", "market_id": o.market, "side": "yes", "action": "sell",
             "role": "maker", "qty": filled, "yes_price": k_fill},
            {"venue": "polymarket_us", "market_id": o.pm_market, "side": "no", "action": "sell",
             "role": "taker", "qty": filled, "yes_price": pm_fill},
        ],
    })
}

// -------------------------------------------------------------- the resting ask ---

/// An exit ask this process has at the venue.
///
/// THE ANSWER TO "a naive placer rests a SECOND ask" (`unwind` §5). The ledger
/// record stays `status: "open"` until the unwind books, so the next scan
/// re-selects the same basket; this is the only thing in the process that knows
/// an exit is already working it, and [`Live::target`] consults it before
/// anything else.
#[derive(Debug, Clone)]
pub struct Resting {
    pub order: Order,
    /// The venue's id — what a cancel and a fill read are addressed to.
    pub venue_order_id: String,
    /// Our id, the one `gateway::is_ours` must recognise or the kill sweep
    /// leaves it resting.
    pub client_order_id: String,
    pub since: Instant,
}

/// Everything the armed pass keeps between cycles.
pub struct Live {
    /// DECIDE, LOG, AND STOP. Everything above the wire runs — the view, the
    /// debounce, the ledger basis, the limit, every refusal — and the order is
    /// printed instead of sent.
    ///
    /// It exists for `positions::Act::shadow`'s reason, doubled: this prices off
    /// a two-leg basis reconstruction that has never met live money, and unlike
    /// the naked-leg completer it RESTS rather than crossing, so the first live
    /// one is also the first time anyone sees what price it picks.
    pub shadow: bool,
    pub ledger_path: String,
    pub debounce: Debounce,
    pub resting: Option<Resting>,
    cx: Cx,
    fees: FeeSchedule,
    /// Markets the venue has refused as halted, and until when. Same policy as
    /// `positions::Act::parked` — `engine::hedge::venue_reopen_park` — so there
    /// is ONE halt backoff in this binary rather than three.
    parked: BTreeMap<String, (Instant, u32)>,
}

impl Live {
    pub fn new(shadow: bool, ledger_path: String) -> Live {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        Live {
            shadow,
            ledger_path,
            debounce: Debounce::default(),
            resting: None,
            cx,
            fees,
            parked: BTreeMap::new(),
        }
    }

    pub fn is_parked(&self, market: &str) -> bool {
        self.parked.get(market).is_some_and(|(until, _)| *until > Instant::now())
    }

    pub fn park(&mut self, market: &str) -> Duration {
        let strikes = self.parked.get(market).map(|(_, s)| *s).unwrap_or(0) + 1;
        let d = crate::engine::hedge::venue_reopen_park(strikes);
        self.parked.insert(market.to_string(), (Instant::now() + d, strikes));
        d
    }

    /// Which candidate, if any, this cycle may work — the gate that makes
    /// "rest a second ask" unexpressible.
    ///
    /// `Err` carries the reason so the operator can tell a quiet book from a
    /// held one; `Ok(None)` never happens, which is why there is no third arm.
    pub fn target<'a>(
        &mut self,
        exits: &'a [crate::unwind::Exit],
        now: f64,
    ) -> Result<&'a crate::unwind::Exit, String> {
        // The debounce is folded on EVERY cycle, including one that will not
        // place: forgetting a scan because an exit was already resting would
        // restart the clock for every other candidate.
        let admitted = self.debounce.admit(exits, now);
        if self.resting.is_some() {
            return Err(format!(
                "an exit is already resting ({} of {} in-scope candidate(s) held) — \
                 MAX_RESTING is {MAX_RESTING}, so nothing else may rest until it fills or is \
                 pulled",
                admitted.len(),
                exits.len()
            ));
        }
        let Some(e) = admitted.into_iter().next() else {
            return Err(format!(
                "{} candidate(s), none held for {DEBOUNCE_S:.0}s across {DEBOUNCE_SCANS} \
                 scans — 60% of every excursion above the floor on the live tape is one \
                 sample long",
                exits.len()
            ));
        };
        if self.is_parked(&e.market_id) {
            return Err(format!(
                "{} is halted at the venue; not re-sending into it",
                e.market_id
            ));
        }
        Ok(e)
    }

}

/// `x` + millis — a SEPARATE id space from the engine's `m`/`h`/`t` and the
/// recon pass's `n`, so a post-mortem can tell whose order was whose.
/// `gateway::is_ours` must recognise it or the kill sweep would not clean it up.
pub fn client_order_id() -> String {
    format!(
        "x{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    )
}

/// One line describing what was decided, used by both the shadow and the armed
/// path so the two can never drift.
pub fn describe(o: &Order, shadow: bool, held_s: f64) -> String {
    format!(
        "[maker-exit]{} REST ASK {}x {} @ {} (closes the lot opened at ts {}, kalshi basis \
         {}/ct + pm basis {}/ct, pm ask {} at decision, locks >= {}/ct; held {:.0}s) — id {}",
        if shadow { " SHADOW —" } else { "" },
        o.qty,
        o.market,
        o.limit,
        o.closes_ts,
        o.k_basis,
        o.pm_basis,
        o.pm_ask_at_decision,
        MIN_LOCK,
        held_s,
        o.rel_id,
    )
}

/// Book a filled-and-closed exit. Through `append_basket`, never a raw append:
/// the single-author rule is what caught the 2026-07-30 double-book.
pub fn book(path: &str, o: &Order, k_fill: &str, pm_fill: &str, filled: i64, ts: f64) -> String {
    let rec = close_record(o, k_fill, pm_fill, filled, ts);
    match ledger::append_basket(path, rec) {
        Ok(ledger::Booking::Booked) => {
            CLOSED.fetch_add(1, AtomicOrd::Relaxed);
            format!(
                "[maker-exit] CLOSED {filled}x {} at {k_fill} / pm {pm_fill} — the lot opened \
                 at ts {} is unwound by {filled}",
                o.market, o.closes_ts
            )
        }
        Ok(ledger::Booking::AlreadyBooked) => {
            CLOSED.fetch_add(1, AtomicOrd::Relaxed);
            format!(
                "[maker-exit] CLOSED {filled}x {} but a record with this exact \
                 (relationship, ts) is ALREADY in {path} — not written twice. This should be \
                 unreachable; if it is not, two writers share a clock.",
                o.market
            )
        }
        Ok(ledger::Booking::Contested(others)) => {
            CLOSED.fetch_add(1, AtomicOrd::Relaxed);
            format!(
                "[maker-exit] CONTESTED {} — another writer booked an OPEN basket on the same \
                 markets at ts {others:?} while this exit was closing one. BOTH are in the \
                 ledger. If arbbot-hedge.timer or --positions-recon-act is armed it may have \
                 re-opened what this just closed; reconcile {} by hand.",
                o.rel_id, o.market
            )
        }
        Err(e) => {
            UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
            format!(
                "[maker-exit] LEDGER WRITE FAILED ({e}) — {filled}x {} IS CLOSED AT BOTH \
                 VENUES AND IS NOT BOOKED. Every exposure fold still believes this basket is \
                 open, so the caps are reserved against a position that no longer exists. \
                 FIX BY HAND. maker_exit_unresolved is now {}.",
                o.market,
                unresolved()
            )
        }
    }
}

/// The alarm for a Kalshi exit that filled and whose PM-US leg did not close.
///
/// Its own function because it is the one outcome here that is not a non-event,
/// and the wording is the deliverable: a naked leg the ledger does not know
/// about is worse than either half alone.
pub fn alarm_unresolved(o: &Order, filled: i64, why: &str) -> String {
    UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
    format!(
        "[maker-exit] ### NAKED AFTER EXIT ### {filled}x {} SOLD on Kalshi and the PM-US \
         close on {} did NOT complete ({why}). We are short {} PM-US YES with nothing against \
         it, and the ledger still says the basket opened at ts {} is OPEN — so no exposure \
         fold, no cap and no unwind can see this. --positions-recon-act will report it as a \
         PmShort next cycle and, if armed, will try to complete it by BUYING KALSHI BACK, \
         which re-opens what this just exited. RECONCILE BY HAND. \
         maker_exit_unresolved is now {}.",
        o.market,
        o.pm_market,
        filled,
        o.closes_ts,
        unresolved()
    )
}

// ------------------------------------------------------------------- the loop ---

/// How often the exit cycle runs. 60 s — the same tick the engine publishes its
/// view on, and the timescale both inputs move on: `arbbot-marks.timer` rewrites
/// the forward APRs every two minutes and the hurdle moves as baskets book.
/// Nothing here belongs on the book-event path; a resting ask is not a crossing.
pub const CYCLE_S: u64 = 60;
const CYCLE: Duration = Duration::from_secs(CYCLE_S);

/// What the loop needs that is not in [`Live`].
pub struct Cfg {
    pub marks_path: String,
    /// This process's `--rel-prefix` scope, passed to `unwind::select` so a
    /// candidate outside it is marked unactionable rather than silently dropped.
    pub rel_prefixes: Vec<String>,
    /// Relationship -> PM-US market. From the same registry read
    /// `positions::pairs_from_registry` does, so the two cannot disagree about
    /// which PM-US market a basket's other leg is.
    pub pm_market: BTreeMap<String, String>,
}

/// The armed loop.
///
/// It reads through the SINKS rather than building its own gateways, for
/// `spawn_positions_recon`'s reason: one gateway per venue per process is one
/// shared token bucket (quirk `xv-shared-api-budget`), and a private gateway
/// here would be a second bucket against the same account.
pub async fn exit_loop(
    mut live: Live,
    cfg: Cfg,
    kalshi: std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: std::sync::Arc<dyn crate::sink::OrderSink>,
) {
    let mut iv = tokio::time::interval(CYCLE);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        iv.tick().await;
        for line in cycle(&mut live, &cfg, &kalshi, &pmus).await {
            eprintln!("{line}");
        }
    }
}

/// One cycle. Returns the lines the operator should see, for `unwind_tick`'s
/// reason: a feature whose only output is `eprintln!` has nothing a test can
/// assert on.
async fn cycle(
    live: &mut Live,
    cfg: &Cfg,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    let mut out = Vec::new();
    let view = match engine_view() {
        Ok(v) => v,
        Err(why) => {
            // A view we cannot believe is not a reason to leave an ask resting
            // against it. PULL IT. The engine stops publishing when it is killed
            // or feed-pulled, so this is also how a halted engine retires an
            // exit that it can no longer value.
            if live.resting.is_some() {
                out.push(format!("[maker-exit] engine view unusable ({why}) — pulling the ask"));
                out.extend(pull(live, kalshi).await);
            }
            request_suppress(BTreeSet::new());
            return out;
        }
    };
    // MANAGE FIRST. A resting ask is money already at a venue; a new candidate
    // is not, and `MAX_RESTING` means nothing new can rest until this is done.
    if live.resting.is_some() {
        out.extend(manage(live, &view, kalshi, pmus).await);
    }

    let marks = std::fs::read_to_string(&cfg.marks_path).unwrap_or_default();
    let sel = crate::unwind::select(
        &marks,
        view.apr_bar,
        view.global_cap_usd,
        &cfg.rel_prefixes,
        wall_now(),
    );
    let exits = match sel {
        Ok((e, _)) => e,
        Err(why) => {
            out.push(format!("[maker-exit] NO SCAN — cannot decide: {why}"));
            // Keep suppressing whatever is resting; select nothing new.
            request_suppress(live.resting.iter().map(|r| r.order.market.clone()).collect());
            return out;
        }
    };
    let now = wall_now();
    let target = match live.target(&exits, now) {
        Ok(e) => e.clone(),
        Err(why) => {
            request_suppress(live.resting.iter().map(|r| r.order.market.clone()).collect());
            out.push(format!("[maker-exit] nothing to rest: {why}"));
            return out;
        }
    };
    // THE ENTRY QUOTE COMES OFF FIRST. Published before the decision, so the
    // first cycle that picks a market ASKS and the next one places — which is
    // exactly what `decide`'s settle guard refuses on, by name.
    let mut want: BTreeSet<String> = live.resting.iter().map(|r| r.order.market.clone()).collect();
    want.insert(target.market_id.clone());
    request_suppress(want);

    let Some(pm) = cfg.pm_market.get(&target.rel_id).cloned() else {
        out.push(refuse(format!(
            "[maker-exit] NO — {} has no polymarket_us leg in the registry, so there is no \
             close leg to price",
            target.rel_id
        )));
        return out;
    };
    let market = target.market_id.clone();
    let k = kalshi.clone();
    let quote = match tokio::task::spawn_blocking(move || k.market_quote(&market)).await {
        Ok(Ok(q)) => q,
        Ok(Err(e)) => {
            out.push(refuse(format!(
                "[maker-exit] NO QUOTE for {} ({e}) — cannot price an exit",
                target.market_id
            )));
            return out;
        }
        Err(e) => {
            out.push(refuse(format!("[maker-exit] quote task failed for {} ({e})", target.market_id)));
            return out;
        }
    };
    let records = match ledger::read(&live.ledger_path) {
        Ok(r) => r,
        Err(e) => {
            out.push(refuse(format!(
                "[maker-exit] the ledger is unreadable ({e}) — nothing may be priced off it"
            )));
            return out;
        }
    };
    let decided =
        decide(&mut live.cx, &live.fees, &records, &target, &pm, &quote, &view, Instant::now());
    let order = match decided {
        Ok(o) => o,
        Err(why) => {
            out.push(format!("[maker-exit] NO — {}: {why}", target.rel_id));
            return out;
        }
    };
    let held = live.debounce.held_s(&target, now).unwrap_or(0.0);
    out.push(describe(&order, live.shadow, held));
    // THE LAST LINE BEFORE THE WIRE. Everything above ran; nothing below does.
    if live.shadow {
        return out;
    }
    out.extend(place(live, order, kalshi).await);
    out
}

/// Rest ONE post-only GTC ask.
async fn place(
    live: &mut Live,
    order: Order,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    let coid = client_order_id();
    let req = PlaceRequest {
        market: order.market.clone(),
        side: Side::Ask,
        price: order.limit.clone(),
        qty: order.qty,
        tif: Tif::Gtc,
        // POST-ONLY IS LOAD-BEARING, not a preference. It is what makes an ask
        // that would cross a REJECTION rather than a take, which is the whole
        // reason the suppression settle above is allowed to be a bounded wait
        // instead of a proof — and the reason a stale book cannot turn this
        // into the −111%/yr take it exists not to make.
        post_only: true,
        client_order_id: coid.clone(),
    };
    let k = kalshi.clone();
    let r = req.clone();
    match tokio::task::spawn_blocking(move || k.place(&r)).await {
        Ok(Ok(id)) => {
            PLACED.fetch_add(1, AtomicOrd::Relaxed);
            let line = format!(
                "[maker-exit] RESTED {}x {} @ {} — venue id {id}, client {coid}",
                order.qty, order.market, order.limit
            );
            live.resting = Some(Resting {
                order,
                venue_order_id: id,
                client_order_id: coid,
                since: Instant::now(),
            });
            vec![line]
        }
        Ok(Err(e)) if e.retry() == arb_venue::error::Retry::MarketHalted => {
            let d = live.park(&order.market);
            vec![refuse(format!(
                "[maker-exit] {} is HALTED at the venue ({e}) — parked for {}s. No price, size \
                 or interval answers a halt; only the venue reopening does.",
                order.market,
                d.as_secs()
            ))]
        }
        // The venue ANSWERED and its answer was no — including the post-only
        // rejection this order is DESIGNED to earn rather than avoid. Nothing is
        // at the venue and nothing is owed.
        Ok(Err(e @ arb_venue::VenueError::Status { .. })) => vec![refuse(format!(
            "[maker-exit] PLACE REFUSED on {} ({e}) — if this is a post-only rejection the \
             book has come to our price, which is the one outcome that costs nothing",
            order.market
        ))],
        // THE REQUEST NEVER COMPLETED, which is not the same as never happening.
        // A resting order under an id this process never learned is unaddressable
        // by any cancel, and a fill on it would arrive against nothing.
        Ok(Err(e)) => {
            UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
            vec![format!(
                "[maker-exit] PLACE DID NOT COMPLETE on {} ({e}) — an ASK may be RESTING at \
                 the venue under an id this process never learned, so nothing here can cancel \
                 it and a fill on it would leave a naked PM-US short. CHECK BY HAND: \
                 client_order_id {coid}. maker_exit_unresolved is now {}.",
                order.market,
                unresolved()
            )]
        }
        Err(e) => {
            UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
            vec![format!(
                "[maker-exit] PLACE TASK FAILED on {} ({e}) — same as above: the ask may or \
                 may not be resting. CHECK BY HAND: client_order_id {coid}",
                order.market
            )]
        }
    }
}

/// Look after the resting ask: has it filled, and does it still pay?
async fn manage(
    live: &mut Live,
    view: &EngineView,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    let Some(r) = live.resting.clone() else { return Vec::new() };
    let k = kalshi.clone();
    let id = r.venue_order_id.clone();
    let filled = match tokio::task::spawn_blocking(move || k.filled_qty(&id)).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            // NOT "it did not fill". `filled_qty` refuses rather than answering
            // 0 when it cannot ask, and treating that as unfilled is how a real
            // fill goes unclosed and unbooked.
            return vec![format!(
                "[maker-exit] could not read the fill state of {} ({e}) — leaving it resting \
                 and asking again next cycle",
                r.order.market
            )];
        }
        Err(e) => return vec![format!("[maker-exit] fill-read task failed ({e})")],
    };
    if filled >= 1 {
        // A PARTIAL FILL LEAVES THE REST OF THE ASK RESTING, and `close_leg`
        // forgets the order — deliberately, so nothing can poll a spent exit as
        // if it were still working. Forgetting a LIVE order is a different
        // thing: it would keep filling while we close the PM leg, and each
        // extra contract is another naked short that nothing in this process
        // can see, cancel or book. So the remainder comes off FIRST.
        let mut out = vec![format!(
            "[maker-exit] {} filled {filled} of {} — cancelling the remainder before closing, \
             so nothing else can trade while the PM-US leg is open",
            r.order.market, r.order.qty
        )];
        out.extend(cancel_at_venue(kalshi, &r).await);
        // ...and the count is RE-READ after the cancel. Anything that traded
        // between the poll and the cancel is ours too, and closing less than we
        // sold is precisely the naked leg this path exists to avoid.
        let k = kalshi.clone();
        let id = r.venue_order_id.clone();
        let settled = match tokio::task::spawn_blocking(move || k.filled_qty(&id)).await {
            Ok(Ok(n)) => n.max(filled),
            // Unreadable after the cancel. `filled` is a LOWER bound and is
            // still the best number we have, so close that much rather than
            // nothing — and say that the difference, if any, is invisible.
            _ => {
                out.push(format!(
                    "[maker-exit] could not re-read {} after the cancel — closing the {filled} \
                     we know traded. If the cancel raced a further fill, the difference is \
                     naked and unbooked; CHECK BY HAND.",
                    r.order.market
                ));
                filled
            }
        };
        if settled > r.order.qty {
            // The venue reports more filled than we ordered. Same clamp and same
            // refusal to be quiet about it as `positions::place_and_book`: the
            // excess contracts are real and have no lot to book against.
            out.push(alarm_unresolved(
                &r.order,
                settled - r.order.qty,
                "the venue reports MORE filled than the exit ordered",
            ));
        }
        out.extend(close_leg(live, view, &r, settled.min(r.order.qty), pmus).await);
        return out;
    }
    // Unfilled. Does the floor still sit at or below where we are resting? The
    // floor only moves with the ledger basis (fixed) and the PM close (not),
    // so a RISE means the PM book has moved against the exit and the price we
    // are resting at no longer locks anything.
    match still_pays(&mut live.cx, &live.fees, &r.order, view) {
        Ok(()) => Vec::new(),
        Err(why) => {
            let mut out =
                vec![format!("[maker-exit] PULLING {} — {why}", r.order.market)];
            out.extend(pull(live, kalshi).await);
            out
        }
    }
}

/// Does the resting price still clear the floor the PM book now implies?
///
/// It re-derives the floor rather than comparing to the one we placed at,
/// because the floor is a function of a book that moves and a basis that does
/// not. `Ok` means the ask is at or above it and a fill would still lock
/// [`MIN_LOCK`].
fn still_pays(cx: &mut Cx, fees: &FeeSchedule, o: &Order, view: &EngineView) -> Result<(), String> {
    let Some(pm_ask) = view.pm_ask.get(&o.pm_market) else {
        return Err(format!(
            "the PM-US book for {} has gone dark, so the close leg can no longer be valued",
            o.pm_market
        ));
    };
    let (Some(pm_ask_d), Some(k_basis), Some(pm_basis), Some(resting)) = (
        cx.parse(pm_ask),
        cx.parse(&o.k_basis),
        cx.parse(&o.pm_basis),
        cx.parse(&o.limit),
    ) else {
        return Err("a price on the resting exit does not parse".into());
    };
    let tick = cx.parse_exact(TICK);
    let pm_close = cx.add(pm_ask_d, tick);
    // A one-rung penny ladder is enough to re-derive the floor: we are only
    // asking whether the floor has passed the price we are already at, and the
    // real ladder was used to pick that price.
    let ladder = vec![("0.0000".to_string(), "1.0000".to_string(), "0.0100".to_string())];
    let floor = exit_limit(cx, fees, &ladder, k_basis, pm_basis, pm_close, o.qty)?;
    if cx.cmp(resting, floor) == Ordering::Less {
        return Err(format!(
            "the PM-US ask has moved to {pm_ask}, which puts the profit floor at {} — above \
             the {} we are resting at, so a fill here would no longer lock {MIN_LOCK}/ct",
            cx.emit_6dp(floor),
            o.limit
        ));
    }
    Ok(())
}

/// Cancel the resting ask and forget it.
///
/// Forgetting it on a FAILED cancel too, deliberately, and this is the one place
/// that choice is not obviously right. The alternative — keep it and retry — is
/// worse: `filled_qty` is still polled every cycle for as long as we remember
/// it, so a cancel that failed because the order had already filled is caught
/// next cycle either way, while an order the venue has already removed would be
/// retried for ever. What is NOT covered is a cancel that failed and left the
/// order resting: the account-wide sweep at kill and at exit reaches it (it
/// carries an `x` id, which `gateway::is_ours` recognises), and nothing else
/// does.
async fn pull(
    live: &mut Live,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    let Some(r) = live.resting.take() else { return Vec::new() };
    cancel_at_venue(kalshi, &r).await
}

/// Send the cancel, WITHOUT touching [`Live::resting`].
///
/// Split from [`pull`] because the partial-fill path needs the cancel and must
/// NOT drop the record until it has re-read the fill count off the same id.
async fn cancel_at_venue(
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    r: &Resting,
) -> Vec<String> {
    use arb_venue::gateway::{CancelBy, CancelRequest};
    let k = kalshi.clone();
    let req = CancelRequest {
        by: CancelBy::VenueId(r.venue_order_id.clone()),
        market_slug: Some(r.order.market.clone()),
    };
    match tokio::task::spawn_blocking(move || k.cancel(&req)).await {
        Ok(Ok(())) => vec![format!(
            "[maker-exit] pulled {} ({}) after {:.0}s resting",
            r.order.market,
            r.venue_order_id,
            r.since.elapsed().as_secs_f64()
        )],
        Ok(Err(e)) => vec![format!(
            "[maker-exit] CANCEL FAILED on {} ({e}) — order {} may still be RESTING. It \
             carries client id {} and the account-wide sweep will reach it at kill or exit; \
             nothing else will.",
            r.order.market, r.venue_order_id, r.client_order_id
        )],
        Err(e) => vec![format!("[maker-exit] cancel task failed ({e})")],
    }
}

/// The most a PM-US close may pay, checked against the ask the engine can see.
///
/// Split out of [`close_leg`] because it is the whole of `unwind` §5's fourth
/// bullet — "RE-PRICE AT FILL TIME ... AND ABANDON THE CLOSE IF IT NO LONGER
/// PAYS" — and a decision that lives inside an `async fn` that places orders is
/// a decision no test can reach.
fn price_close(
    cx: &mut Cx,
    fees: &FeeSchedule,
    o: &Order,
    filled: i64,
    view: &EngineView,
) -> Result<String, String> {
    let k_fill = cx.parse(&o.limit).ok_or("the fill price does not parse")?;
    let k_basis = cx.parse(&o.k_basis).ok_or("the kalshi basis does not parse")?;
    let pm_basis = cx.parse(&o.pm_basis).ok_or("the pm basis does not parse")?;
    let limit = close_limit(cx, fees, k_fill, k_basis, pm_basis, filled)?;
    let ask = view
        .pm_ask
        .get(&o.pm_market)
        .ok_or_else(|| format!("no PM-US ask for {} in the engine's book", o.pm_market))?;
    let ask_d = cx.parse(ask).ok_or("the pm ask does not parse")?;
    // A limit BELOW the ask cannot fill, and sending one is how a close reports
    // "unfilled" for a book that was never going to take it. Say what is true.
    if cx.cmp(ask_d, limit) == Ordering::Greater {
        return Err(format!(
            "the PM-US ask is {ask} and the most this close may pay is {} — the book has \
             moved against the exit since the ask was rested",
            cx.emit_6dp(limit)
        ));
    }
    // The limit, not the ask: we send at or above the touch, so the worst fill
    // is the limit, and the limit is the price the lock was solved for.
    let limit = cx.quantize_4dp(limit);
    Ok(limit.to_standard_notation_string())
}

/// The Kalshi ask filled. Close the PM-US leg, re-priced against the book AS IT
/// IS NOW, and book the unwind.
///
/// THE PM BOOK HERE IS THE ENGINE'S, not a fresh venue read, and that is a
/// limitation rather than a choice: `PmusGateway` has no `market_quote` and
/// adding one is venue code that could not be exercised without placing. The
/// engine publishes on its 60 s tick and [`VIEW_MAX_AGE`] refuses anything older
/// than three of them, so the close is priced against a book that is at most one
/// tick stale — much fresher than the marks the exit was selected from, and
/// still not the book at the instant of the IOC.
async fn close_leg(
    live: &mut Live,
    view: &EngineView,
    r: &Resting,
    filled: i64,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    // The exit is spent either way: the contracts are gone from Kalshi. Forget
    // the resting order NOW so that no path below can leave it addressable and
    // no later cycle can poll it as if it were still working.
    live.resting = None;
    let mut out = vec![format!(
        "[maker-exit] FILLED {filled}x {} @ {} after {:.0}s resting — closing the PM-US leg \
         on {}",
        r.order.market,
        r.order.limit,
        r.since.elapsed().as_secs_f64(),
        r.order.pm_market
    )];
    let limit = match price_close(&mut live.cx, &live.fees, &r.order, filled, view) {
        Ok(p) => p,
        Err(why) => {
            out.push(alarm_unresolved(&r.order, filled, &why));
            return out;
        }
    };
    let coid = client_order_id();
    let req = PlaceRequest {
        market: r.order.pm_market.clone(),
        // BUYING the YES back is how a short YES is closed.
        side: Side::Bid,
        price: limit.clone(),
        qty: filled,
        tif: Tif::Ioc,
        // Both halves set: the wire builders inline the TIF from `post_only`
        // and ignore `PlaceRequest::tif`, so the two must not be able to drift.
        post_only: false,
        client_order_id: coid.clone(),
    };
    let p = pmus.clone();
    let rq = req.clone();
    let oid = match tokio::task::spawn_blocking(move || p.place(&rq)).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => {
            out.push(alarm_unresolved(&r.order, filled, &format!("PM-US refused the close: {e}")));
            return out;
        }
        Err(e) => {
            out.push(alarm_unresolved(&r.order, filled, &format!("close task failed: {e}")));
            return out;
        }
    };
    let mut got = 0i64;
    let mut unreadable: Option<String> = None;
    for i in 0..crate::naked_act::FILL_POLLS {
        let p = pmus.clone();
        let id = oid.clone();
        match tokio::task::spawn_blocking(move || p.filled_qty(&id)).await {
            Ok(Ok(n)) => {
                got = n;
                unreadable = None;
                if got >= 1 {
                    break;
                }
            }
            Ok(Err(e)) => unreadable = Some(e.to_string()),
            Err(e) => unreadable = Some(format!("fill-read task failed: {e}")),
        }
        if i + 1 < crate::naked_act::FILL_POLLS {
            tokio::time::sleep(crate::naked_act::FILL_POLL_GAP).await;
        }
    }
    if let Some(e) = unreadable {
        out.push(alarm_unresolved(
            &r.order,
            filled,
            &format!("the close IOC {oid} (client {coid}) was ACCEPTED and its fill could not be read: {e}"),
        ));
        return out;
    }
    if got < 1 {
        out.push(alarm_unresolved(&r.order, filled, "the close IOC did not fill"));
        return out;
    }
    let booked = got.min(filled);
    if booked < filled {
        // Part of the exit closed. The remainder is a real naked short, and it
        // is alarmed for exactly what it is rather than folded into the book.
        out.push(alarm_unresolved(
            &r.order,
            filled - booked,
            "the close IOC filled only partly",
        ));
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    out.push(book(&live.ledger_path, &r.order, &r.order.limit, &limit, booked, ts));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unwind::Exit as Cand;

    fn v(s: &str) -> Value {
        serde_json::from_str(s).expect("fixture")
    }

    fn penny() -> Vec<(String, String, String)> {
        vec![("0.0000".into(), "1.0000".into(), "0.0100".into())]
    }

    fn quote(bid: Option<&str>, ask: Option<&str>) -> Quote {
        Quote {
            market: "K-a".into(),
            status: "active".into(),
            yes_bid: bid.map(str::to_string),
            yes_ask: ask.map(str::to_string),
            ladder: penny(),
        }
    }

    fn ready() -> (Cx, FeeSchedule) {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        (cx, fees)
    }

    /// An ordinary open basket in the live ledger's commonest shape: PM-US
    /// short YES at `pm_yes` (so `1 - pm_yes` a contract of NO), Kalshi long YES
    /// at `k_yes`, both maker so the recorded fee is zero-ish.
    fn open_basket(ts: f64, qty: i64, pm_yes: &str, k_yes: &str) -> Value {
        v(&format!(
            r#"{{"ts":{ts},"relationship_id":"r1","status":"open","qty":{qty},"legs":[
                 {{"venue":"polymarket_us","market_id":"p-a","side":"no","role":"maker",
                   "qty":{qty},"yes_price":"{pm_yes}"}},
                 {{"venue":"kalshi","market_id":"K-a","side":"yes","role":"maker",
                   "qty":{qty},"yes_price":"{k_yes}"}}]}}"#
        ))
    }

    fn cand(qty: i64, opened_ts: f64) -> Cand {
        Cand {
            rel_id: "r1".into(),
            market_id: "K-a".into(),
            opened_ts,
            qty,
            fwd_apr: 11.0,
            exit_ct: 0.03,
            actionable: true,
            near_floor: false,
            resolves_estimated: true,
        }
    }

    /// A view with the market already yielded long enough to place.
    fn view(pm_ask: &str) -> EngineView {
        EngineView {
            apr_bar: 16.0,
            global_cap_usd: 500.0,
            pm_ask: [("p-a".to_string(), pm_ask.to_string())].into_iter().collect(),
            suppressed_at: [(
                "K-a".to_string(),
                Instant::now() - Duration::from_secs_f64(SUPPRESS_SETTLE_S + 1.0),
            )]
            .into_iter()
            .collect(),
        }
    }

    // ---- the debounce -----------------------------------------------------

    /// THE FLAP, AS A TEST. On `data/exec/marks_history.jsonl` (4,735 samples,
    /// 172.7 h) 122 of the 202 excursions above the two-tick floor are ONE
    /// SAMPLE LONG — the median excursion has no duration at all. A placer
    /// without a debounce rests an ask against every one of them.
    ///
    /// This pins both halves of the rule: a candidate that appears and vanishes
    /// is never admitted, and its clock RESTARTS if it comes back, because a
    /// debounce that remembers a spike across its own absence re-admits it.
    #[test]
    fn a_one_sample_spike_never_reaches_the_venue_and_its_clock_restarts() {
        let mut d = Debounce::default();
        let c = [cand(10, 1.0)];
        let t0 = 1_000_000.0;
        assert!(d.admit(&c, t0).is_empty(), "the first sighting is never enough");
        // ...and one sample later it is gone, exactly like 60% of the tape.
        assert!(d.admit(&[], t0 + 120.0).is_empty());
        // It comes back much later. The clock must start again from HERE, not
        // resume: it has been continuously selected for 0 seconds.
        let t1 = t0 + 100_000.0;
        assert!(d.admit(&c, t1).is_empty(), "a returning spike is a new candidate");
        assert!(
            d.admit(&c, t1 + DEBOUNCE_S).is_empty(),
            "two samples DEBOUNCE_S apart is not {DEBOUNCE_SCANS} scans"
        );
        let out = d.admit(&c, t1 + DEBOUNCE_S + 1.0);
        assert_eq!(out.len(), 1, "held long enough, across enough scans: {out:?}");
    }

    /// BOTH CONDITIONS BIND, and each one alone lets a different failure
    /// through. Time alone is satisfiable by two samples straddling a stalled
    /// loop; scans alone are satisfiable by three samples six minutes apart,
    /// which is inside the run length of two thirds of the tape's excursions.
    #[test]
    fn neither_the_clock_nor_the_scan_count_is_sufficient_alone() {
        let c = [cand(10, 1.0)];

        // enough scans, not enough time: three scans a minute apart.
        let mut d = Debounce::default();
        for i in 0..8 {
            assert!(
                d.admit(&c, 1_000_000.0 + f64::from(i) * 60.0).is_empty(),
                "8 scans over 420s is under DEBOUNCE_S ({DEBOUNCE_S})"
            );
        }

        // enough time, not enough scans: two samples DEBOUNCE_S apart.
        let mut d = Debounce::default();
        assert!(d.admit(&c, 1_000_000.0).is_empty());
        assert!(
            d.admit(&c, 1_000_000.0 + DEBOUNCE_S + 1.0).is_empty(),
            "2 scans is under DEBOUNCE_SCANS ({DEBOUNCE_SCANS})"
        );
    }

    /// THE BASKET IS THE KEY, NOT THE RELATIONSHIP. The live france book
    /// carries seven separate `brunoretailleau` lots opened on different days;
    /// keyed on the relationship, one lot's persistence would admit a lot first
    /// seen this instant.
    #[test]
    fn one_lots_persistence_does_not_admit_another_lot_on_the_same_relationship() {
        let mut d = Debounce::default();
        let old = cand(10, 1784646659.716);
        let fresh = cand(10, 1784727137.511);
        let t0 = 1_000_000.0;
        for i in 0..4 {
            d.admit(std::slice::from_ref(&old), t0 + f64::from(i) * DEBOUNCE_S / 2.0);
        }
        let both = [old.clone(), fresh.clone()];
        let out = d.admit(&both, t0 + 2.0 * DEBOUNCE_S);
        let ids: Vec<f64> = out.iter().map(|e| e.opened_ts).collect();
        assert_eq!(ids, vec![old.opened_ts], "only the held lot: {ids:?}");
    }

    // ---- one ask ----------------------------------------------------------

    /// THE SECOND ASK IS UNEXPRESSIBLE. `unwind::select` re-selects the same
    /// basket on every scan — the ledger record stays `status: "open"` until the
    /// unwind books — so without this a placer rests another ask every cycle.
    #[test]
    fn nothing_rests_while_an_exit_is_already_outstanding() {
        let mut l = Live::new(true, "/dev/null".into());
        let c = [cand(10, 1.0), cand(10, 2.0)];
        let t0 = 1_000_000.0;
        for i in 0..3 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S);
        }
        let e = l.target(&c, t0 + 3.0 * DEBOUNCE_S).expect("held long enough");
        assert_eq!(e.opened_ts, 1.0, "the first admitted candidate");

        l.resting = Some(Resting {
            order: Order {
                rel_id: "r1".into(),
                market: "K-a".into(),
                pm_market: "p-a".into(),
                qty: 5,
                limit: "0.5000".into(),
                closes_ts: 1.0,
                k_basis: "0.19".into(),
                pm_basis: "0.78".into(),
                pm_ask_at_decision: "0.20".into(),
            },
            venue_order_id: "v1".into(),
            client_order_id: "x1".into(),
            since: Instant::now(),
        });
        let why = l.target(&c, t0 + 4.0 * DEBOUNCE_S).expect_err("one at a time");
        assert!(why.contains("already resting"), "{why}");
        assert!(why.contains("MAX_RESTING"), "and it names the cap: {why}");
    }

    /// The debounce is still folded on a cycle that cannot place, or every
    /// other candidate's clock restarts each time one exit rests.
    #[test]
    fn a_blocked_cycle_still_counts_as_a_scan() {
        let mut l = Live::new(true, "/dev/null".into());
        let c = [cand(10, 1.0)];
        let t0 = 1_000_000.0;
        l.resting = Some(Resting {
            order: Order {
                rel_id: "r1".into(),
                market: "K-a".into(),
                pm_market: "p-a".into(),
                qty: 5,
                limit: "0.5000".into(),
                closes_ts: 1.0,
                k_basis: "0.19".into(),
                pm_basis: "0.78".into(),
                pm_ask_at_decision: "0.20".into(),
            },
            venue_order_id: "v1".into(),
            client_order_id: "x1".into(),
            since: Instant::now(),
        });
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S);
        }
        l.resting = None;
        let e = l.target(&c, t0 + 4.0 * DEBOUNCE_S).expect("the clock kept running");
        assert_eq!(e.rel_id, "r1");
    }

    // ---- the price --------------------------------------------------------

    /// THE EXIT MUST PAY AGAINST WHAT WE PAID, BOTH LEGS.
    ///
    /// A basket bought at Kalshi YES 0.19 + PM NO 0.78 cost $0.97 a contract.
    /// With the PM YES offered at 0.20 the close pays 1 − 0.21 = 0.79 back, so
    /// the Kalshi ask has to fetch at least 0.97 + 0.005 − 0.79 = 0.185 before
    /// fees, and a cent more once the PM taker fee and the Kalshi maker fee are
    /// charged. The floor is whatever clears it; what this pins is that the
    /// floor MOVES with the basis and with the PM book, in the right direction
    /// and by the right sign.
    #[test]
    fn the_floor_rises_with_the_basis_and_falls_as_the_pm_close_gets_cheaper() {
        let (mut cx, fees) = ready();
        let l = penny();
        let px = |cx: &mut Cx, k: &str, p: &str, close: &str| {
            let (kb, pb, pc) = (cx.parse_exact(k), cx.parse_exact(p), cx.parse_exact(close));
            exit_limit(cx, &fees, &l, kb, pb, pc, 5).map(|d| cx.emit_6dp(d))
        };
        let base = px(&mut cx, "0.19", "0.78", "0.21").expect("solvable");
        // A DEARER lot needs a dearer exit.
        let dearer = px(&mut cx, "0.25", "0.78", "0.21").expect("solvable");
        assert!(dearer > base, "dearer basis must ask more: {dearer} vs {base}");
        // A CHEAPER close returns more, so the Kalshi leg may ask less.
        let cheaper_close = px(&mut cx, "0.19", "0.78", "0.15").expect("solvable");
        assert!(
            cheaper_close < base,
            "a cheaper PM close must lower the floor: {cheaper_close} vs {base}"
        );
        // ...and the floor really does cover the whole basket. Sell at it, close
        // at the priced PM level, and the lock survives.
        let limit = cx.parse_exact(&base);
        let k_fee = {
            let size = cx.from_i64(5);
            let t = fees.fee(&mut cx, Venue::Kalshi, Role::Maker, limit, size, "");
            cx.div(t, size)
        };
        let pm_close = cx.parse_exact("0.21");
        let pm_fee = {
            let size = cx.from_i64(5);
            let t = fees.fee(&mut cx, Venue::PolymarketUs, Role::Taker, pm_close, size, "");
            cx.div(t, size)
        };
        let proceeds = cx.sub(limit, k_fee);
        let back = cx.one_minus(pm_close);
        let back = cx.sub(back, pm_fee);
        let proceeds = cx.add(proceeds, back);
        let paid = cx.parse_exact("0.97");
        let net = cx.sub(proceeds, paid);
        let lock = cx.parse_exact(MIN_LOCK);
        assert!(
            cx.cmp(net, lock) != Ordering::Less,
            "the floor must actually lock {MIN_LOCK}: net {}",
            cx.emit_6dp(net)
        );
    }

    /// A LOT THAT CANNOT BE EXITED AT A PROFIT IS REFUSED, NOT DUMPED.
    ///
    /// A BASKET COSTING MORE THAN A DOLLAR IS NOT AUTOMATICALLY UNEXITABLE, and
    /// the first draft of this test assumed it was. We hold BOTH sides, so the
    /// exit is worth `k_sell + (1 − p_close)` — which exceeds $1 exactly when
    /// the two venues disagree, which is the whole reason the basket exists. A
    /// $1.15 basket against a PM YES at $0.30 exits at $0.48 and pays.
    ///
    /// What makes a lot unexitable is the basis being over a dollar AND the leg
    /// we hold having gone the wrong way: paid $0.85 for a PM NO now worth 5c,
    /// so recovering the lot would need a Kalshi ask above $1. That is the
    /// −111%/yr shape, and a maker exit is not a slow way to take it.
    #[test]
    fn a_lot_that_cannot_be_exited_above_a_dollar_is_refused() {
        let (mut cx, fees) = ready();
        let (kb, pb, pc) =
            (cx.parse_exact("0.30"), cx.parse_exact("0.85"), cx.parse_exact("0.95"));
        let e = exit_limit(&mut cx, &fees, &penny(), kb, pb, pc, 5)
            .expect_err("a $1.15 lot whose PM leg is worth 5c cannot be recovered under $1");
        assert!(e.contains("at or above $1"), "{e}");
        assert!(e.contains("cannot be done"), "{e}");

        // ...and the mirror: when the PM leg alone more than repays the lot,
        // the floor is the smallest POSITIVE tick and never $0.00, which is not
        // a price but an offer to give the contracts away.
        let (kb, pb, pc) =
            (cx.parse_exact("0.05"), cx.parse_exact("0.10"), cx.parse_exact("0.01"));
        let l = exit_limit(&mut cx, &fees, &penny(), kb, pb, pc, 5).expect("anything pays");
        assert_eq!(cx.emit_6dp(l), "0.010000", "a floor of $0.00 is not a placeable ask");
    }

    /// THE FLOOR IS THE **LOWEST** PRICE THAT PAYS, AND THE TICK BELOW IT DOES
    /// NOT.
    ///
    /// This is the test the profit lock actually needed, and the first draft did
    /// not have it: a mutation setting `MIN_LOCK` to zero left every other test
    /// in this module green, because on any single basis the cent grid usually
    /// swallows half a cent of lock. Only the BOUNDARY sees it, and only over a
    /// sweep — the two floors differ for the bases where 0.005 straddles a tick,
    /// which is about half of them.
    ///
    /// Both halves are load-bearing. Without the first the floor could be
    /// anything above the true one (cautious, but it would stop selecting);
    /// without the second it could be anything below (which is a fill that does
    /// not pay).
    #[test]
    fn the_floor_is_the_lowest_tick_that_locks_min_lock_and_the_one_below_it_does_not() {
        let (mut cx, fees) = ready();
        let l = penny();
        let tick = cx.parse_exact(TICK);
        let lock = cx.parse_exact(MIN_LOCK);
        let mut straddled = 0usize;
        // SUB-CENT STEPS, and that is the whole point of the sweep. A basis that
        // moves in whole cents moves `want` in whole cents too, so its distance
        // to the next tick never changes and a half-cent lock is invisible at
        // every point — which is exactly how the first version of this sweep
        // scored GREEN against a mutation that deleted the lock. A ledger basis
        // is not on the cent grid anyway: `worst_lot` adds a fee per contract.
        for milli in 0..40 {
            let k_basis = cx.parse_exact(&format!("0.{:04}", 1900 + milli));
            let pm_basis = cx.parse_exact("0.78");
            let pm_close = cx.parse_exact("0.21");
            let Ok(floor) = exit_limit(&mut cx, &fees, &l, k_basis, pm_basis, pm_close, 5) else {
                continue;
            };
            let net = |cx: &mut Cx, px: D| {
                let size = cx.from_i64(5);
                let kf = fees.fee(cx, Venue::Kalshi, Role::Maker, px, size, "");
                let kf = cx.div(kf, size);
                let pf = fees.fee(cx, Venue::PolymarketUs, Role::Taker, pm_close, size, "");
                let pf = cx.div(pf, size);
                let out = cx.sub(px, kf);
                let back = cx.one_minus(pm_close);
                let back = cx.sub(back, pf);
                let out = cx.add(out, back);
                let paid = cx.add(k_basis, pm_basis);
                cx.sub(out, paid)
            };
            let at = net(&mut cx, floor);
            assert!(
                cx.cmp(at, lock) != Ordering::Less,
                "k_basis 0.{:04}: the floor {} nets only {}",
                1900 + milli,
                cx.emit_6dp(floor),
                cx.emit_6dp(at)
            );
            let below = cx.sub(floor, tick);
            if !cx.is_pos(below) {
                continue;
            }
            let under = net(&mut cx, below);
            assert!(
                cx.cmp(under, lock) == Ordering::Less,
                "k_basis 0.{:04}: one tick below the floor ({}) still nets {} — the floor is \
                 not the LOWEST price that pays, so the exit is asking for more than the \
                 policy requires and will select nothing on a thin book",
                1900 + milli,
                cx.emit_6dp(below),
                cx.emit_6dp(under)
            );
            straddled += 1;
        }
        assert!(straddled >= 10, "the sweep must actually exercise the boundary: {straddled}");
    }

    /// THE KALSHI LEG IS CHARGED THE **MAKER** FEE, because it rests. Charging
    /// the taker schedule would price an order that cannot happen — a post-only
    /// ask never crosses — and would do it in the expensive direction, silently
    /// selecting nothing on exactly the thin books this is for.
    #[test]
    fn the_resting_leg_is_priced_at_the_maker_schedule_not_the_taker_one() {
        let (mut cx, fees) = ready();
        let l = penny();
        let (kb, pb, pc) =
            (cx.parse_exact("0.19"), cx.parse_exact("0.78"), cx.parse_exact("0.21"));
        let limit = exit_limit(&mut cx, &fees, &l, kb, pb, pc, 5).expect("solvable");
        let size = cx.from_i64(5);
        let maker = fees.fee(&mut cx, Venue::Kalshi, Role::Maker, limit, size, "");
        let taker = fees.fee(&mut cx, Venue::Kalshi, Role::Taker, limit, size, "");
        assert!(
            cx.cmp(maker, taker) == Ordering::Less,
            "the two schedules must differ or this test proves nothing: {} vs {}",
            cx.emit_6dp(maker),
            cx.emit_6dp(taker)
        );
    }

    /// THE FILL-TIME RE-PRICE (`unwind` §5's fourth bullet). What the Kalshi
    /// leg actually returned is a FACT by then, so the question flips: how dear
    /// may the PM YES be and still leave the lock? A book that has moved against
    /// us past that point is refused — and refusing means staying naked, which
    /// is why the caller alarms.
    #[test]
    fn the_close_is_repriced_against_what_the_kalshi_leg_actually_returned() {
        let (mut cx, fees) = ready();
        let (kb, pb) = (cx.parse_exact("0.19"), cx.parse_exact("0.78"));
        let good = cx.parse_exact("0.25");
        let p = close_limit(&mut cx, &fees, good, kb, pb, 5).expect("a 0.25 fill closes");
        let p_s = cx.emit_6dp(p);

        // A BETTER Kalshi fill tolerates a DEARER close.
        let better = cx.parse_exact("0.30");
        let p2 = close_limit(&mut cx, &fees, better, kb, pb, 5).expect("still closes");
        let p2_s = cx.emit_6dp(p2);
        assert!(p2_s > p_s, "a better fill buys more room: {p2_s} vs {p_s}");

        // A 97c basket can ALWAYS be closed at some PM price, because it cost
        // less than the dollar the pair pays out — a 2c Kalshi fill just means
        // the close has to be found under 4c. The first draft of this test
        // asserted an error there and was wrong about the arithmetic.
        let thin = cx.parse_exact("0.02");
        let p = close_limit(&mut cx, &fees, thin, kb, pb, 5).expect("still closes, barely");
        let five = cx.parse_exact("0.05");
        assert!(cx.cmp(p, five) == Ordering::Less, "{}", cx.emit_6dp(p));

        // What has NO close is a lot that cost more than a dollar and whose
        // Kalshi leg came back small: no positive PM price leaves the lock.
        let (kb2, pb2) = (cx.parse_exact("0.30"), cx.parse_exact("0.85"));
        let e = close_limit(&mut cx, &fees, thin, kb2, pb2, 5)
            .expect_err("$1.15 of basis against a 2c fill closes nothing");
        assert!(e.contains("no PM-US price"), "{e}");
    }

    // ---- the decision -----------------------------------------------------

    /// THE WHOLE DECISION, on the live ledger's shape.
    #[test]
    fn a_held_candidate_with_a_ledger_basis_and_a_yielded_ask_prices_an_exit() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let o = decide(
            &mut cx,
            &fees,
            &recs,
            &cand(34, 1.0),
            "p-a",
            &quote(Some("0.10"), Some("0.30")),
            &view("0.20"),
            Instant::now(),
        )
        .expect("a priced exit");
        assert_eq!(o.qty, MAX_CLIP, "capped at the clip, not the 34-lot");
        assert_eq!(o.closes_ts, 1.0, "ONE lot, so ONE closes_ts — no split");
        // 0.19 PLUS the Kalshi MAKER fee the schedule charges on a 34-lot at
        // that price, per contract. The basis is all-in — `worst_lot` computes
        // the fee when the ledger leg does not carry one — because a hedge or
        // an exit that beats a fee-free basis has not beaten anything.
        assert_eq!(o.k_basis, "0.192941");
        assert_eq!(o.pm_basis, "0.780000", "1 - 0.22, the cost of the NO");
        assert_eq!(o.pm_market, "p-a");
        assert!(o.limit.parse::<f64>().unwrap() > 0.10, "and it does not cross: {}", o.limit);
    }

    /// NO BASIS IS A NAMED REFUSAL, NEVER A DEFAULT — on EITHER leg. The
    /// `xvus-fedcut-26` incident is the PM leg's version of this; a Kalshi leg
    /// with no open record is the same fact about the other side.
    #[test]
    fn a_lot_our_own_ledger_cannot_vouch_for_is_never_exited() {
        let (mut cx, fees) = ready();
        // fully unwound: the relationship vouches for nothing
        let recs = vec![
            open_basket(1.0, 5, "0.22", "0.19"),
            v(r#"{"ts":2.0,"relationship_id":"r1","status":"unwound","closes_ts":1.0,"qty":5}"#),
        ];
        let e = decide(
            &mut cx,
            &fees,
            &recs,
            &cand(5, 1.0),
            "p-a",
            &quote(Some("0.10"), Some("0.30")),
            &view("0.20"),
            Instant::now(),
        )
        .expect_err("no open lot");
        assert!(e.contains("unaccounted for"), "{e}");
        assert!(e.contains("none will be invented"), "{e}");

        // ...and a PM leg pointing the other way (an inverted basket, 9 of them
        // in the live file) is not this trade either.
        let inverted = vec![v(
            r#"{"ts":1.0,"relationship_id":"r1","status":"open","qty":5,"legs":[
                 {"venue":"polymarket_us","market_id":"p-a","side":"yes","role":"taker",
                  "qty":5,"yes_price":"0.22"},
                 {"venue":"kalshi","market_id":"K-a","side":"yes","role":"taker",
                  "qty":5,"yes_price":"0.19"}]}"#,
        )];
        let e = decide(
            &mut cx,
            &fees,
            &inverted,
            &cand(5, 1.0),
            "p-a",
            &quote(Some("0.10"), Some("0.30")),
            &view("0.20"),
            Instant::now(),
        )
        .expect_err("an inverted basket has no PM short to close");
        assert!(e.contains("PM-US leg"), "the refusal names the side: {e}");
    }

    /// THE ENTRY QUOTE COMES OFF FIRST, AND A BOUNDED WAIT FOLLOWS IT.
    /// `scripts/unwind_positions.py:45-49` — card ed6a5910, 14 soft unwinds
    /// deadlocked by self-trade prevention.
    #[test]
    fn nothing_rests_until_the_entry_quoter_has_had_time_to_yield_the_ask() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let never = EngineView { suppressed_at: BTreeMap::new(), ..view("0.20") };
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &never, Instant::now(),
        )
        .expect_err("the quoter has not been told");
        assert!(e.contains("yield"), "{e}");

        let just_now = EngineView {
            suppressed_at: [("K-a".to_string(), Instant::now())].into_iter().collect(),
            ..view("0.20")
        };
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &just_now, Instant::now(),
        )
        .expect_err("told, but not yet settled");
        assert!(e.contains("giving it"), "{e}");
    }

    /// AN ASK AT OR UNDER THE BID IS THE TAKE, AND THE TAKE IS THE TRADE THIS
    /// MODULE EXISTS NOT TO MAKE. Post-only would reject it; if it did not it
    /// would cross into a book at a price nothing here decided to take.
    #[test]
    fn an_exit_that_would_cross_the_book_is_refused_rather_than_crossed() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        // Bid at 0.90 is far above any floor this basket produces.
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.9000"), Some("0.9500")), &view("0.20"), Instant::now(),
        )
        .expect_err("that is a take");
        assert!(e.contains("CROSS"), "{e}");
    }

    /// A MARKET THE VENUE IS NOT TRADING TAKES NO ORDER, whatever the marks
    /// file says. This is the guard `unwind::Skip::NotPriceable` structurally
    /// cannot be: inside the 15 minutes before a frozen marks file ages out, a
    /// priced row is not evidence of a live book.
    #[test]
    fn a_market_the_venue_is_not_trading_is_never_rested_into() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let mut q = quote(Some("0.10"), Some("0.30"));
        q.status = "finalized".into();
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a", &q, &view("0.20"), Instant::now(),
        )
        .expect_err("a finalized market takes no order");
        assert!(e.contains("not `active`"), "{e}");
    }

    /// A CANDIDATE OUTSIDE THIS PROCESS'S SCOPE IS NEVER ACTED ON. `unwind` §1
    /// is half retracted — the armed process now runs `--rel-prefix xvus-` and
    /// every priced position is `xvus-` — but the test it rested on is ENFORCED
    /// here rather than assumed, because the scope is an operator flag and can
    /// change back with one line of a drop-in.
    #[test]
    fn a_candidate_outside_the_rel_prefix_scope_is_never_exited() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let mut c = cand(34, 1.0);
        c.actionable = false;
        let e = decide(
            &mut cx, &fees, &recs, &c, "p-a",
            &quote(Some("0.10"), Some("0.30")), &view("0.20"), Instant::now(),
        )
        .expect_err("not ours");
        assert!(e.contains("--rel-prefix"), "{e}");
    }

    /// A DARK PM-US BOOK MAKES THE CLOSE UNPRICEABLE, SO THE EXIT IS
    /// UNPRICEABLE. Resting the Kalshi ask anyway would be committing to a
    /// two-leg trade having valued one leg.
    #[test]
    fn an_exit_whose_close_leg_has_no_book_is_refused() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let dark = EngineView { pm_ask: BTreeMap::new(), ..view("0.20") };
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &dark, Instant::now(),
        )
        .expect_err("no PM ask");
        assert!(e.contains("unpriceable"), "{e}");
        assert!(e.contains("one-legged trade"), "and says why that matters: {e}");
    }

    // ---- the view ---------------------------------------------------------

    /// A SILENT OR STALE ENGINE DECIDES NOTHING. The view carries the hurdle,
    /// the cap and the PM book; the honest answer to "what is the hurdle" from
    /// an engine that has stopped publishing is not a number. It is also how a
    /// KILLED engine stops this module — the publisher does not run while
    /// `killed` or `feed_reason` is set, so the view ages out.
    #[test]
    fn a_silent_or_stale_engine_view_refuses_rather_than_defaulting() {
        let _g = test_serial();
        reset_view();
        let e = engine_view().expect_err("never published");
        assert!(e.contains("never published"), "{e}");
        assert!(e.contains("will not be invented"), "{e}");

        publish_view(view("0.20"));
        assert_eq!(engine_view().expect("fresh").apr_bar, 16.0);
        reset_view();
    }

    // ---- the record -------------------------------------------------------

    /// THE CLOSING RECORD NAMES EXACTLY ONE OPEN RECORD, which is what
    /// `ledger::open_exposure` nets against. A record naming a relationship and
    /// not a `closes_ts` frees exposure that is still on.
    #[test]
    fn the_close_books_against_the_one_lot_it_was_sized_to() {
        let o = Order {
            rel_id: "r1".into(),
            market: "K-a".into(),
            pm_market: "p-a".into(),
            qty: 5,
            limit: "0.2100".into(),
            closes_ts: 1.0,
            k_basis: "0.190000".into(),
            pm_basis: "0.780000".into(),
            pm_ask_at_decision: "0.20".into(),
        };
        let rec = close_record(&o, "0.2100", "0.2000", 3, 99.0);
        assert_eq!(rec["status"], "unwound");
        assert_eq!(rec["closes_ts"], 1.0);
        assert_eq!(rec["qty"], 3);
        assert_eq!(rec["source"], ledger::SOURCE);
        assert_eq!(rec["strategy"], "maker-exit");
        assert_eq!(rec["fees_pending"], true, "the venue fees are not read here");
        assert!(rec.get("realized_pnl_usd").is_none(), "no confident wrong number");

        // ...and it really nets against the record it names.
        let open = open_basket(1.0, 5, "0.22", "0.19");
        let e = ledger::open_exposure(vec![open, rec]);
        assert_eq!(e.get("r1"), Some(&2.0), "5 open, 3 unwound: {e:?}");
    }

    /// THE ONE OUTCOME THAT IS NOT A NON-EVENT. A Kalshi exit that filled and
    /// whose PM-US close did not complete leaves a real short with the ledger
    /// still saying the basket is hedged, and it must never share a counter
    /// with "the book did not pay".
    #[test]
    fn a_filled_exit_whose_close_failed_alarms_on_its_own_gauge() {
        let o = Order {
            rel_id: "r1".into(),
            market: "K-a".into(),
            pm_market: "p-a".into(),
            qty: 5,
            limit: "0.2100".into(),
            closes_ts: 1.0,
            k_basis: "0.190000".into(),
            pm_basis: "0.780000".into(),
            pm_ask_at_decision: "0.20".into(),
        };
        let before = unresolved();
        let refused_before = refused();
        let line = alarm_unresolved(&o, 3, "PM-US returned 503");
        assert_eq!(unresolved(), before + 1);
        assert_eq!(refused(), refused_before, "a naked leg is NOT a refusal");
        assert!(line.contains("NAKED AFTER EXIT"), "{line}");
        assert!(line.contains("RECONCILE BY HAND"), "{line}");
        assert!(
            line.contains("BUYING KALSHI BACK"),
            "and it names the fight with --positions-recon-act: {line}"
        );
    }

    // ---- the fill path ----------------------------------------------------

    /// A venue that answers a scripted sequence and records the order of the
    /// calls, because the ORDER is the property under test: a cancel that
    /// happens after the close is not the same safety measure as one before it.
    #[derive(Default)]
    struct FakeVenue {
        /// `filled_qty` answers, consumed in order; the last one repeats.
        fills: Mutex<Vec<i64>>,
        log: Mutex<Vec<String>>,
    }

    impl FakeVenue {
        fn with_fills(v: &[i64]) -> std::sync::Arc<FakeVenue> {
            std::sync::Arc::new(FakeVenue {
                fills: Mutex::new(v.to_vec()),
                log: Mutex::new(Vec::new()),
            })
        }
        fn note(&self, s: String) {
            self.log.lock().unwrap().push(s);
        }
        fn calls(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    impl crate::sink::OrderSink for FakeVenue {
        fn place(
            &self,
            r: &arb_venue::gateway::PlaceRequest,
        ) -> Result<String, arb_venue::VenueError> {
            self.note(format!("place {} {}x @{}", r.market, r.qty, r.price));
            Ok("v1".into())
        }
        fn cancel(
            &self,
            _r: &arb_venue::gateway::CancelRequest,
        ) -> Result<(), arb_venue::VenueError> {
            self.note("cancel".into());
            Ok(())
        }
        fn cancel_all_open(&self) -> Result<(), arb_venue::VenueError> {
            unreachable!("this path never sweeps")
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, arb_venue::VenueError> {
            unreachable!("this path never lists")
        }
        fn filled_qty(&self, _id: &str) -> Result<i64, arb_venue::VenueError> {
            let mut f = self.fills.lock().unwrap();
            let n = if f.len() > 1 { f.remove(0) } else { *f.first().unwrap_or(&0) };
            self.note(format!("filled_qty -> {n}"));
            Ok(n)
        }
    }

    fn resting_exit(qty: i64) -> Resting {
        Resting {
            order: Order {
                rel_id: "r1".into(),
                market: "K-a".into(),
                pm_market: "p-a".into(),
                qty,
                // 0.21 against a 0.19+0.78 basis: comfortably above the floor,
                // so the close leg prices without complaint.
                limit: "0.2500".into(),
                closes_ts: 1.0,
                k_basis: "0.190000".into(),
                pm_basis: "0.780000".into(),
                pm_ask_at_decision: "0.20".into(),
            },
            venue_order_id: "v1".into(),
            client_order_id: "x1".into(),
            since: Instant::now(),
        }
    }

    fn ledger_scratch(tag: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("arb-maker-exit-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.join("trades.jsonl").to_string_lossy().into_owned()
    }

    /// A PARTIALLY FILLED EXIT HAS ITS REMAINDER CANCELLED **BEFORE** THE PM-US
    /// LEG IS CLOSED, AND THE FILL COUNT IS RE-READ AFTER THE CANCEL.
    ///
    /// The defect this pins was live in the first draft: `close_leg` drops the
    /// record of the resting order — it must, or a spent exit would be polled
    /// for ever — so a 5-lot ask that filled 2 left THREE contracts resting at
    /// the venue that nothing in this process could see, cancel or book. They
    /// would keep trading while the PM-US IOC was in flight, and every one of
    /// them is another naked short.
    ///
    /// Two assertions, and the second is the one that is easy to lose: the
    /// cancel must come first, AND the count must be read AGAIN afterwards.
    /// Closing the number we read BEFORE the cancel closes less than we sold
    /// whenever the two race, which is the same naked leg by a smaller amount.
    #[tokio::test]
    async fn a_partial_fill_cancels_the_remainder_before_closing_and_re_reads_the_count() {
        // The ASYNC serial: these hold the guard across venue awaits, and a sync
        // MutexGuard held over an await is both a clippy error and a real
        // deadlock risk on a single-threaded runtime.
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        // 2 filled on the first read, 3 by the time the cancel lands.
        let kalshi = FakeVenue::with_fills(&[2, 3]);
        let pmus = FakeVenue::with_fills(&[3]);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_scratch("partial");
        let mut live = Live::new(false, path.clone());
        live.resting = Some(resting_exit(5));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        let ks = kalshi.calls();
        let cancel_at = ks.iter().position(|c| c == "cancel").unwrap_or_else(|| panic!("no cancel: {ks:?}"));
        assert_eq!(cancel_at, 1, "the cancel must follow the FIRST read: {ks:?}");
        assert_eq!(
            ks.len(),
            3,
            "and a SECOND read must follow the cancel — closing the pre-cancel count closes \
             less than we sold: {ks:?}"
        );
        assert_eq!(ks[2], "filled_qty -> 3", "{ks:?}");

        // The PM close is for the RE-READ count, not the first one.
        let ps = pmus.calls();
        assert!(ps[0].starts_with("place p-a 3x"), "closed the wrong size: {ps:?}");

        // ...and the exit is retired, so nothing polls it again.
        assert!(live.resting.is_none(), "a spent exit must not be polled next cycle");
        assert!(out.contains("cancelling the remainder"), "{out}");

        // The unwind is booked against the ONE lot, for what actually traded.
        let recs = crate::ledger::read(&path).expect("clean ledger");
        assert_eq!(recs.len(), 1, "{recs:?}");
        assert_eq!(recs[0]["status"], "unwound");
        assert_eq!(recs[0]["qty"], 3);
        assert_eq!(recs[0]["closes_ts"], 1.0);
        let _ = std::fs::remove_file(&path);
    }

    /// AN UNFILLED EXIT IS LEFT ALONE WHILE IT STILL PAYS, AND PULLED WHEN IT
    /// STOPS.
    ///
    /// The floor is re-derived from the PM book every cycle rather than compared
    /// to the price we placed at, because the basis is fixed and the book is
    /// not: an ask that locked half a cent when it was rested locks nothing once
    /// the PM-US YES has run up, and a resting order nobody re-checks is a
    /// standing offer to lose money at a price we chose an hour ago.
    #[tokio::test]
    async fn an_unfilled_exit_rests_on_while_it_pays_and_is_pulled_when_it_stops() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let mut live = Live::new(false, ledger_scratch("unfilled"));
        live.resting = Some(resting_exit(5));
        let out = manage(&mut live, &view("0.20"), &k, &p).await;
        assert!(out.is_empty(), "a paying exit is left alone, silently: {out:?}");
        assert!(live.resting.is_some());
        assert!(!kalshi.calls().iter().any(|c| c == "cancel"), "{:?}", kalshi.calls());

        // The PM-US ask runs from 0.20 to 0.60: the close now costs 40c more, so
        // the floor climbs past the 0.25 we are resting at.
        let out = manage(&mut live, &view("0.60"), &k, &p).await.join("\n");
        assert!(out.contains("PULLING"), "{out}");
        assert!(out.contains("no longer lock"), "and it says why: {out}");
        assert!(live.resting.is_none(), "the ask is retired");
        assert!(kalshi.calls().iter().any(|c| c == "cancel"), "{:?}", kalshi.calls());
    }

    /// A CLOSE THAT CANNOT BE MADE LEAVES US NAKED, AND SAYS SO ON THE
    /// MUST-STAY-0 GAUGE RATHER THAN BOOKING ANYTHING.
    ///
    /// This is the abandon half of `unwind` §5's fourth bullet, and the point is
    /// what abandoning MEANS here: the Kalshi contracts are already gone, so
    /// refusing the close is not declining a trade, it is choosing to stay
    /// one-legged rather than pay a price that turns the exit into a loss.
    /// Nothing may be booked — a ledger record for a half-done unwind would free
    /// exposure that is still on.
    #[tokio::test]
    async fn a_close_the_book_has_run_away_from_alarms_and_books_nothing() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[5]);
        let pmus = FakeVenue::with_fills(&[0]);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_scratch("naked");
        let mut live = Live::new(false, path.clone());
        live.resting = Some(resting_exit(5));
        let before = unresolved();
        // The PM-US YES is at 0.90: buying it back costs far more than the
        // 0.25 Kalshi fill left room for.
        let out = manage(&mut live, &view("0.90"), &k, &p).await.join("\n");

        assert!(out.contains("NAKED AFTER EXIT"), "{out}");
        assert_eq!(unresolved(), before + 1, "the must-stay-0 gauge moved");
        assert!(pmus.calls().is_empty(), "no IOC may be sent at a price that loses: {out}");
        assert!(
            !std::path::Path::new(&path).exists(),
            "a half-done unwind must not be booked — the exposure is still on"
        );
    }

    /// OUR ID MUST BE SWEEPABLE. `gateway::is_ours` gates the kill sweep and the
    /// shutdown sweep; an id it does not recognise is an order this process
    /// leaves resting on the way out.
    #[test]
    fn the_exit_order_id_is_recognised_by_the_sweep() {
        let id = client_order_id();
        assert!(id.starts_with('x'), "{id}");
        assert!(
            arb_venue::gateway::is_ours(&id),
            "{id} would survive the shutdown sweep — that is exit code 17 territory"
        );
    }
}
