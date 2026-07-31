//! ACTING on a naked leg the venue-truth reconciliation has CONFIRMED — the
//! half `positions.rs` deliberately left out, behind its own flag
//! (`--positions-recon-act`).
//!
//! `positions.rs` is the read: every 5 minutes it pulls actual positions from
//! both venues and finds nakedness however it arose. It says of itself that it
//! "does not place, size, price or book anything", because at the time it landed
//! `arbbot-hedge.timer` (`scripts/hedge_naked_legs.py`, frozen Python) owned
//! acting and two hedgers on one Kalshi key is a DOUBLE HEDGE, not redundancy.
//! This module is the reviewed change that decision was deferred to. It does not
//! make the second hedger safe — nothing here can — it makes this one able to
//! replace the other, so that the cutover is a swap and not an addition.
//!
//! # THE ONE THING THIS PORTS DIFFERENTLY, AND WHY IT IS THE WHOLE POINT
//!
//! The Python prices its hedge off PM-US's `costPerShare`:
//!
//! ```text
//!   pm_basis = Decimal(str(ppos[ps]["cost_per_share"]))   # NO cost/ct = 1 - sold YES px
//!   limit    = 1 - pm_basis - kalshi_taker_fee(ask) - MIN_LOCK
//! ```
//!
//! **That comment is false.** `costPerShare` is `baseCost / |net|` — a RESIDUAL,
//! not a price — and it blows up as `net` approaches zero, which is exactly the
//! regime a hedger reads it in: a leg is naked precisely when its net has been
//! whittled down. Live, on 2026-07-31, `xvus-fedcut-26-usfed-2026-cut` reported
//! `pm basis 9.4` — nine dollars and forty cents a contract, on an instrument
//! that caps at one dollar. The limit that falls out of it is `-8.415`, no ask
//! can ever be at or below that, and so the timer printed
//! "waiting for a better book" every five minutes while the leg sat naked for
//! 58 hours. A hedger that cannot compute its own basis does not fail loudly; it
//! fails as patience.
//!
//! So the basis here comes from OUR OWN LEDGER — the open record whose leg we
//! are completing, at the price we recorded paying — and if no such record can
//! be found the answer is a REFUSAL that says so, never a hedge at a guessed
//! price. See [`worst_lot`]. That refusal is not hypothetical either: the fedcut
//! leg above has NO open record in `data/exec/trades.jsonl` at all (checked
//! 2026-07-31: its whole history is unwound), so this module will report it and
//! decline to hedge it. That is the correct answer to "we hold a position we
//! cannot account for", and it is a different answer from the Python's silence.
//!
//! # WHAT ELSE DIVERGES FROM THE PYTHON, EACH IN THE SAFE DIRECTION
//!
//!   * **The fee is evaluated at the LIMIT, not at the ask.** We send a limit
//!     at or above the ask, so the worst fill is the limit; `p + fee(p)` is
//!     strictly increasing, so pricing off the ask understates the cost of every
//!     fill above it. [`buy_limit`] solves the fixed point instead.
//!   * **The fee is the schedule's, not the Python's approximation.** Kalshi
//!     rounds a taker fee UP to the cent over the whole clip
//!     (`FeeSchedule::fee`); `kalshi_taker_fee` in the script quantizes
//!     `0.07·p·(1-p)` to 4dp per contract and so under-states it — 1.12c where
//!     the venue charges 2c on a single contract at $0.19.
//!   * **An empty side is not a price of zero.** Kalshi spells "no ask" as
//!     `"0.0000"` (1,742 of the 35,640 market rows captured in
//!     `data/catalogs/`), and `Decimal("0.0000") <= limit` is TRUE for every
//!     limit this ever computes. See `arb_venue::gateway::Quote`.
//!   * **The tick comes from the rung the price is in.** `kalshi_ask` takes the
//!     FIRST rung's step and uses it at every price; 7,224 of those 35,640
//!     markets are tapered, so on one of them it prices a $0.42 order to the
//!     tenth of a cent — which the venue rejects, leaving the leg naked. See
//!     [`floor_to_tick`].
//!   * **A Kalshi-long naked leg is acted on, not only printed.** The Python
//!     refuses these entirely ("not auto-hedged (v1)"), which is why two of them
//!     have stood for more than 30 hours. `arb_core::naked::imbalance_action`
//!     gives the three reasons; the second — no cost basis on that side — is the
//!     one that survives adding an order path, and it is exactly what the ledger
//!     basis above answers. See [`sell_limit`].
//!
//! # WHAT THIS STILL CANNOT DO
//!
//! It cannot interlock with `arbbot-hedge.timer`. [`Inflight`] closes the
//! in-process race against this engine's own hedge obligations, and
//! [`contested_by_other_author`] closes the "the timer already booked this leg
//! minutes ago" case, but two processes that read positions at the same instant
//! and place at the same instant are not separable by anything either can see —
//! that is the 600ms double-book of 2026-07-30, and `ledger::append_basket`
//! catches it AFTER the money is spent. **The timer must be stopped before this
//! is armed.** `main` says so in the banner and this module cannot enforce it.

use crate::ledger;
use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use arb_venue::gateway::Quote;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

// ------------------------------------------------------------------- policy ---

/// Minimum locked profit per contract before an order is worth sending.
/// `MIN_LOCK` from `hedge_naked_legs.py:38`, ported verbatim.
pub const MIN_LOCK: &str = "0.005";

/// Contracts in ONE order this module sends.
///
/// Five, and single digits is the point rather than the number. Everything here
/// is priced off a basis reconstructed from a ledger record rather than off a
/// fill we watched, and the reconstruction is the part that has never run
/// against live money. A cap this size bounds the cost of it being wrong at a
/// few dollars, and the cycle repeats every five minutes, so a genuinely
/// profitable 20-contract hole still closes — in four passes, each of which the
/// venue re-confirms.
pub const MAX_CLIP: i64 = 5;

/// Orders ONE cycle may send, across every finding.
///
/// Two. The failure this bounds is not a bad price, it is a bad READ: a venue
/// snapshot that is wrong about many pairs at once is wrong about all of them
/// for the same reason, and a cap of two turns "the position endpoint had a bad
/// morning" into two orders instead of thirty. Guard 3 in `positions.rs` already
/// requires the same finding twice, five minutes apart; this is what bounds the
/// blast radius when both reads are wrong the same way.
pub const MAX_ACTIONS_PER_CYCLE: usize = 2;

/// How long the fill of an IOC is waited for before the order is treated as
/// dead — `for _ in range(6): ... time.sleep(0.5)` from `hedge_naked_legs.py`.
pub const FILL_POLLS: u32 = 6;
pub const FILL_POLL_GAP: std::time::Duration = std::time::Duration::from_millis(500);

/// Ticks the limit search will walk before giving up. The fee is at most 1.75c
/// per contract (the `0.07·p·(1-p)` peak at p=0.5) and the coarsest ladder rung
/// is a cent, so a solution is within two steps whenever one exists; 40 is a
/// termination guard, not a budget.
const MAX_TICK_WALK: u32 = 40;

// ------------------------------------------------------------- the interlock ---

/// Kalshi markets on which THIS PROCESS has a live hedge obligation, as of the
/// last engine hedge tick.
///
/// THE 2026-07-30 INCIDENT, and the reason this is a hard gate rather than a
/// heuristic. Two processes each placed 1x against one PM-US short and the
/// account ended long against itself; `hedges_overfilled` still read 0, because
/// the second fill was credited to an obligation the first had already
/// discharged. `engine/hedge.rs` documents the same shape from the inside
/// (`HedgeOrder::supersedes`) and `orphan.rs`'s startup banner names it. The
/// engine's own obligations are the half of that race this process CAN see, so
/// it must.
///
/// PUBLISHED WHOLESALE, once per engine hedge tick (1 Hz), rather than
/// maintained by paired enter/leave calls at the four `pending_hedges` mutation
/// sites. A missed `leave` would be a market this module never touches again; a
/// missed `enter` would be a double hedge. Overwriting the whole set from the
/// one place that owns it cannot express either mistake.
///
/// It is sound at 1 Hz against a 5-minute cycle for a reason worth stating: an
/// obligation that lives less than one publication is one this module could not
/// act against anyway, because acting requires the SAME imbalance confirmed on
/// two cycles five minutes apart, and a discharged obligation changes the
/// imbalance. The obligations that are genuinely dangerous — parked on a halted
/// market, or waiting for a price inside the slip budget — are the long-lived
/// ones, and those are published hundreds of times over.
#[derive(Debug, Default)]
pub struct Inflight {
    markets: BTreeSet<String>,
    at: Option<std::time::Instant>,
}

static INFLIGHT: Mutex<Option<Inflight>> = Mutex::new(None);

/// How stale a publication may be and still be believed. Five ticks of the 1 Hz
/// publisher: enough that ordinary scheduling jitter never trips it, little
/// enough that a wedged decision loop does.
const INFLIGHT_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(5);

/// Publish the markets with a live hedge obligation. Called from
/// `Engine::hedge_tick` and nowhere else.
pub fn publish_inflight(markets: BTreeSet<String>) {
    if let Ok(mut g) = INFLIGHT.lock() {
        *g = Some(Inflight { markets, at: Some(std::time::Instant::now()) });
    }
}

/// May this module place on `market`?
///
/// FAIL-CLOSED on both "never published" and "stale", and that is deliberate.
/// The question being asked is "is somebody else already working this leg", and
/// the honest answer to it when the publisher is silent is "I cannot tell" —
/// which, for a decision whose wrong answer is a double hedge, is a refusal. The
/// cost of the refusal is that a leg stays naked and keeps being reported, which
/// is the state it is already in.
pub fn inflight_check(market: &str) -> Result<(), String> {
    let g = INFLIGHT.lock().map_err(|_| "in-flight registry poisoned".to_string())?;
    let Some(f) = g.as_ref() else {
        return Err(
            "the engine's hedge tick has never published its live obligations — refusing to \
             place against a set that may not be empty"
                .into(),
        );
    };
    let age = f.at.map(|t| t.elapsed()).unwrap_or(INFLIGHT_MAX_AGE);
    if age > INFLIGHT_MAX_AGE {
        return Err(format!(
            "the engine's live-obligation set is {:.0}s old (the hedge tick is 1 Hz) — the \
             decision loop is not running, so nothing here can tell whether this leg is \
             already being hedged",
            age.as_secs_f64()
        ));
    }
    if f.markets.contains(market) {
        return Err(format!(
            "this engine already owes a hedge on {market} — placing a second one is the \
             2026-07-30 double hedge, and `hedges_overfilled` would not see it"
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn reset_inflight() {
    if let Ok(mut g) = INFLIGHT.lock() {
        *g = None;
    }
}

/// [`INFLIGHT`] is process-wide and `cargo test` runs every test in one process,
/// so a test that publishes an empty set can clear the very interlock another
/// one is asserting on. It did: the double-hedge test let a place through on the
/// first run. Every test in ANY module that touches the registry takes this
/// first — which is why it lives here rather than inside `mod tests`.
///
/// Async, because `positions`' act tests hold it across the cycle they are
/// measuring and the workspace denies `await_holding_lock`.
#[cfg(test)]
pub(crate) static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ------------------------------------------------------------------ the lot ---

/// Which way round a ledger leg has to be for it to be the leg we are working.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    /// We SOLD yes / bought no. The PM-US half of an ordinary basket, and the
    /// side a `PmShort` naked leg is missing its partner for. Cost per contract
    /// is `1 - yes_price`.
    ShortYes,
    /// We BOUGHT yes. The Kalshi half, and the side a `KalshiLong` naked leg has
    /// too much of. Cost per contract is `yes_price`.
    LongYes,
}

impl Held {
    /// The leg `side` spellings that mean this direction.
    ///
    /// THREE VOCABULARIES IN ONE FILE, all present in the live ledger: the
    /// Python writes the POSITION (`yes` / `no`), this engine writes the ORDER
    /// it sent (`bid` / `ask`, because `BookSide` renames to the wire spelling),
    /// and `ledger::identity` already documents the pair. Anything else — the
    /// two `mixed` legs in the file, a spelling nobody has seen — matches
    /// neither arm and the record is skipped, which is the fail-closed reading:
    /// a leg whose direction we cannot name is not a leg we may price off.
    fn matches(self, side: &str) -> bool {
        match self {
            Held::ShortYes => side == "no" || side == "ask",
            Held::LongYes => side == "yes" || side == "bid",
        }
    }
}

/// One still-open ledger lot: what a contract of it cost us, and how many of it
/// are left.
#[derive(Debug, Clone, PartialEq)]
pub struct Lot {
    /// `ts` of the OPEN record. This is half of `(relationship_id, ts)`, which
    /// is the key every correction and unwind addresses a basket by — so it is
    /// also what a closing record has to name.
    pub open_ts: f64,
    /// All-in cost per contract, fees included: the number a completion has to
    /// beat.
    pub cost_per_ct: String,
    /// Contracts of this record still open, after unwinds.
    pub qty: i64,
}

/// The DEAREST still-open lot for one leg of one relationship — the basis a
/// completion must clear.
///
/// # Why the dearest and not an average
///
/// Contracts are fungible: nothing in the ledger says WHICH of our open lots the
/// uncovered contracts came from. An average would price the hedge against a
/// basket we might not be completing; the dearest lot is the only choice that
/// makes the completed basket profitable under every attribution. It costs us
/// hedges we could have justified against a cheaper lot, and that is the right
/// way round — the cost of waiting is a leg that stays naked and stays reported,
/// and the cost of guessing is a basket booked at a loss.
///
/// It is also what gives a Kalshi-long close somewhere to book against:
/// [`Lot::open_ts`] is the record a partial unwind names in `closes_ts`.
///
/// # What makes a record unusable, every case being a refusal and not a default
///
///   * no open record for the relationship at all — the position is real at the
///     venue and unaccounted for in our books, which is a fact to report, not a
///     basis to invent;
///   * the leg is on the market but pointing the WRONG WAY (an inverted basket:
///     9 open records in the live file hold `kalshi side=no` against
///     `polymarket_us side=yes`). Skipped, because completing one of those is
///     not the trade this is;
///   * no price field. `yes_price` or `avg_price` — the two the file actually
///     carries — and nothing else. Two legs in the live file have neither.
///
/// Fees are taken from the leg's own `fees` field when it has one (40 legs in
/// the live file do), and computed from the schedule at the leg's `role` when it
/// does not. A missing `role` is read as `Taker`, the DEARER of the two, for the
/// same reason everything else here rounds against us.
pub fn worst_lot(
    cx: &mut Cx,
    fees: &FeeSchedule,
    records: &[Value],
    rel: &str,
    venue: Venue,
    market: &str,
    held: Held,
) -> Result<Lot, String> {
    let mut best: Option<(D, Lot)> = None;
    let mut skipped_direction = 0usize;
    let mut skipped_price = 0usize;
    for (open_ts, qty, rec) in open_lots(records, rel) {
        let Some(legs) = rec.get("legs").and_then(|v| v.as_array()) else { continue };
        let Some(leg) = legs.iter().find(|l| {
            l.get("venue").and_then(|v| v.as_str()) == Some(venue.as_str())
                && l.get("market_id").and_then(|v| v.as_str()) == Some(market)
        }) else {
            continue;
        };
        let side = leg.get("side").and_then(|v| v.as_str()).unwrap_or("");
        if !held.matches(side) {
            skipped_direction += 1;
            continue;
        }
        let Some(px) = str_field(leg, "yes_price").or_else(|| str_field(leg, "avg_price")) else {
            skipped_price += 1;
            continue;
        };
        let Some(px) = cx.parse(&px) else {
            skipped_price += 1;
            continue;
        };
        // The YES price is what the ledger records on both sides; the COST of a
        // short-yes contract is what is left of the dollar.
        let paid = match held {
            Held::ShortYes => cx.one_minus(px),
            Held::LongYes => px,
        };
        let leg_qty = leg.get("qty").and_then(|v| v.as_f64()).unwrap_or(qty as f64);
        if leg_qty <= 0.0 {
            skipped_price += 1;
            continue;
        }
        let fee_total = match str_field(leg, "fees").and_then(|s| cx.parse(&s)) {
            Some(f) => f,
            None => {
                let role = match leg.get("role").and_then(|v| v.as_str()) {
                    Some("maker") => Role::Maker,
                    // `None` and the two `maker+taker` legs both land here: the
                    // dearer schedule, because a fee we guessed low is a basis
                    // we beat by less than we thought.
                    _ => Role::Taker,
                };
                let size = cx.parse(&leg_qty.to_string()).unwrap_or_else(|| cx.zero());
                fees.fee(cx, venue, role, px, size, "")
            }
        };
        let size = cx.parse(&leg_qty.to_string()).unwrap_or_else(|| cx.zero());
        let fee_ct = cx.div(fee_total, size);
        let cost = cx.add(paid, fee_ct);
        let lot = Lot {
            open_ts,
            cost_per_ct: cx.emit_6dp(cost),
            qty,
        };
        best = match best {
            // Ties keep the EARLIER record: `open_lots` yields file order, so
            // this is deterministic against a ledger holding two identical lots.
            Some((c, l)) if cx.cmp(cost, c) != Ordering::Greater => Some((c, l)),
            _ => Some((cost, lot)),
        };
    }
    best.map(|(_, l)| l).ok_or_else(|| {
        format!(
            "no open ledger lot vouches for {market} on {rel} (skipped: {skipped_direction} \
             pointing the other way, {skipped_price} with no usable price) — the position is \
             real at the venue and unaccounted for in data/exec/trades.jsonl, so there is no \
             basis to price a completion against and none will be invented",
            market = market,
            rel = rel,
        )
    })
}

/// `(ts, remaining qty, record)` for every still-open record of `rel`.
///
/// The same fold `ledger::open_exposure` performs — corrections merged in,
/// unwinds netted off — kept separate only because that one returns totals per
/// relationship and this needs the records themselves.
fn open_lots(records: &[Value], rel: &str) -> Vec<(f64, i64, Value)> {
    let folded = ledger::apply_corrections(records.to_vec());
    let mut unwound: BTreeMap<u64, f64> = BTreeMap::new();
    for r in &folded {
        if r.get("status").and_then(|v| v.as_str()) == Some("unwound")
            && r.get("relationship_id").and_then(|v| v.as_str()) == Some(rel)
        {
            if let Some(k) = r.get("closes_ts").and_then(|v| v.as_f64()) {
                *unwound.entry(k.to_bits()).or_default() +=
                    r.get("qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
            }
        }
    }
    let mut out = Vec::new();
    for r in folded {
        if r.get("status").and_then(|v| v.as_str()) != Some("open")
            || r.get("relationship_id").and_then(|v| v.as_str()) != Some(rel)
        {
            continue;
        }
        let Some(ts) = r.get("ts").and_then(|v| v.as_f64()) else { continue };
        let qty = r.get("qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let closed = unwound.get(&ts.to_bits()).copied().unwrap_or(0.0);
        let remaining = qty - closed;
        if remaining >= 1.0 {
            out.push((ts, remaining as i64, r));
        }
    }
    out
}

/// A leg field that is money: a string, or a JSON number the writer emitted
/// unquoted. Both shapes are in the live file.
fn str_field(leg: &Value, name: &str) -> Option<String> {
    match leg.get(name)? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Open baskets on this relationship written by SOMEBODY ELSE inside the contest
/// window — i.e. `arbbot-hedge.timer` may have completed this leg already.
///
/// The window is `ledger::CONTEST_WINDOW_S`, which is 300s because that IS the
/// timer's period. It closes the case where the timer hedged the leg and the
/// venue position read has not caught up; it does NOT close the case where both
/// processes read and place at the same instant. Nothing either of them can see
/// closes that one — see the module header.
pub fn contested_by_other_author(records: &[Value], rel: &str, now: f64) -> Vec<f64> {
    records
        .iter()
        .filter(|r| r.get("relationship_id").and_then(|v| v.as_str()) == Some(rel))
        .filter(|r| r.get("status").and_then(|v| v.as_str()) == Some("open"))
        .filter(|r| r.get("source").and_then(|v| v.as_str()) != Some(ledger::SOURCE))
        .filter_map(|r| r.get("ts").and_then(|v| v.as_f64()))
        .filter(|t| (now - t).abs() <= ledger::CONTEST_WINDOW_S)
        .collect()
}

// ------------------------------------------------------------ the tick ladder ---

/// The rung `[start, end)` containing `x`, as `(start, step)`.
///
/// A price outside every rung has no tick and therefore no legal spelling, so it
/// is `None` rather than clamped: clamping would send a price the caller did not
/// compute.
fn rung(cx: &mut Cx, ladder: &[(String, String, String)], x: D) -> Option<(D, D)> {
    for (s, e, st) in ladder {
        let (s, e, st) = (cx.parse(s)?, cx.parse(e)?, cx.parse(st)?);
        if cx.cmp(x, s) != Ordering::Less && cx.cmp(x, e) == Ordering::Less {
            return Some((s, st));
        }
    }
    None
}

/// The largest legal price at or below `x`.
///
/// Range-RELATIVE (`start + n·step`), not `floor(x/step)·step`, because a rung
/// whose start is not a multiple of its own step would otherwise produce a price
/// off the ladder. Kalshi's captured rungs all happen to start on a multiple, so
/// today the two agree; the ladder is the venue's to change and this is the form
/// that stays correct when it does.
pub fn floor_to_tick(cx: &mut Cx, ladder: &[(String, String, String)], x: D) -> Option<D> {
    let (start, step) = rung(cx, ladder, x)?;
    let over = cx.sub(x, start);
    let n = cx.div(over, step);
    let n = cx.quantize_int_down(n);
    let back = cx.mul(n, step);
    Some(cx.add(start, back))
}

/// The smallest legal price at or above `x`.
pub fn ceil_to_tick(cx: &mut Cx, ladder: &[(String, String, String)], x: D) -> Option<D> {
    let (start, step) = rung(cx, ladder, x)?;
    let over = cx.sub(x, start);
    let n = cx.div(over, step);
    let n = cx.quantize_int_ceil(n);
    let back = cx.mul(n, step);
    Some(cx.add(start, back))
}

/// The NEXT legal price below `x`, where `x` is already tick-aligned.
///
/// It has to be the next one and not merely a lower one, and the difference
/// shows up exactly at a rung boundary. `floor_to_tick(x - step_at(x))` reads
/// like the obvious spelling and is wrong there: from $0.1000 on a tapered
/// ladder it subtracts the CENT tick of the rung it is leaving and lands on
/// $0.09, skipping the nine deci-cent prices in between. That is safe for a buy
/// limit — every skipped price is one we would have accepted — but it is a
/// cent of edge given away for nothing, and on the sell side the mirror of it
/// would be a cent demanded for nothing. So the bottom of a rung steps into the
/// TOP tick of the rung below instead.
fn tick_down(cx: &mut Cx, ladder: &[(String, String, String)], x: D) -> Option<D> {
    let (start, step) = rung(cx, ladder, x)?;
    if cx.cmp(x, start) == Ordering::Greater {
        // Still inside this rung. `x` is aligned, so `x - step >= start`.
        return Some(cx.sub(x, step));
    }
    // `x` IS the bottom of its rung, so the answer belongs to the rung whose
    // exclusive `end` is exactly here. A ladder with a hole below `x` has no
    // next price and says so.
    for (s, e, st) in ladder {
        let (s, e, st) = (cx.parse(s)?, cx.parse(e)?, cx.parse(st)?);
        if cx.cmp(e, x) == Ordering::Equal {
            let top = cx.sub(e, st);
            if cx.cmp(top, s) != Ordering::Less {
                return Some(top);
            }
        }
    }
    None
}

/// The next legal price ABOVE `x`.
///
/// No boundary special case is needed in this direction: `x + step_at(x)` lands
/// at or above the rung's exclusive end only when it has reached the next
/// rung's start, which is itself a legal tick, and `ceil_to_tick` returns it
/// unchanged.
fn tick_up(cx: &mut Cx, ladder: &[(String, String, String)], x: D) -> Option<D> {
    let (_, step) = rung(cx, ladder, x)?;
    let higher = cx.add(x, step);
    ceil_to_tick(cx, ladder, higher)
}

/// Kalshi's taker fee PER CONTRACT on a clip of `qty` at `price`.
///
/// Per contract because that is the unit the basket arithmetic is in, but
/// derived from the whole clip because that is how the venue charges: the
/// schedule ceils to the cent ONCE over `qty·p·(1-p)`, so a 5-lot pays less per
/// contract than a 1-lot does. Sizing before pricing is therefore not an
/// optimisation, it is a precondition.
fn fee_per_ct(cx: &mut Cx, fees: &FeeSchedule, price: D, qty: i64) -> D {
    let size = cx.from_i64(qty);
    let total = fees.fee(cx, Venue::Kalshi, Role::Taker, price, size, "");
    cx.div(total, size)
}

// ---------------------------------------------------------------- the prices ---

/// The highest price at which BUYING the missing Kalshi YES still leaves the
/// completed basket locking `MIN_LOCK` a contract — Case A.
///
/// The rule is `hedge_naked_legs.py:186-187`'s, with the fee evaluated where the
/// fill can actually happen. We send a limit at or above the ask, so the worst
/// fill is the LIMIT; `p + fee(p)` is strictly increasing on `[0,1]` (its
/// derivative is `1 + 0.07·(1-2p)`, positive everywhere), so the condition
///
/// ```text
///   basis + L + fee(L) + MIN_LOCK <= 1
/// ```
///
/// holding at `L` implies it at every reachable fill. The Python evaluates
/// `fee(ask)` instead, which is smaller than `fee(L)` whenever both sit below
/// $0.50 — the ordinary case for a hedge leg — so its limit is too generous by
/// the difference.
///
/// Walking DOWN from the fee-free bound rather than solving in closed form:
/// there is no closed form over a tapered ladder, the walk is at most two steps
/// because the fee is at most 1.75c, and a search that only ever moves in the
/// safe direction cannot overshoot into a price we would not have accepted.
pub fn buy_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    ladder: &[(String, String, String)],
    basis: D,
    qty: i64,
) -> Result<D, String> {
    let lock = cx.parse_exact(MIN_LOCK);
    let one = cx.one;
    let headroom = cx.sub(one, basis);
    let l = cx.sub(headroom, lock);
    if !cx.is_pos(l) {
        return Err(format!(
            "the leg already cost {} a contract, so no Kalshi price completes it under $1 \
             with {MIN_LOCK} locked — this basket cannot be finished at a profit and \
             completing it anyway would be buying a known loss",
            cx.emit_6dp(basis)
        ));
    }
    let Some(mut l_tick) = floor_to_tick(cx, ladder, l) else {
        return Err(format!(
            "{} is outside every rung of the venue's tick ladder, so it has no legal \
             spelling",
            cx.emit_6dp(l)
        ));
    };
    for _ in 0..MAX_TICK_WALK {
        let fee = fee_per_ct(cx, fees, l_tick, qty);
        let cost = cx.add(basis, l_tick);
        let cost = cx.add(cost, fee);
        let cost = cx.add(cost, lock);
        if cx.cmp(cost, one) != Ordering::Greater {
            return Ok(l_tick);
        }
        let Some(next) = tick_down(cx, ladder, l_tick) else {
            return Err("the tick walk fell off the bottom of the ladder".into());
        };
        l_tick = next;
        if !cx.is_pos(l_tick) {
            return Err(format!(
                "no positive Kalshi price completes this basket at a profit against a basis \
                 of {}",
                cx.emit_6dp(basis)
            ));
        }
    }
    Err("the limit search did not converge — refusing rather than guessing".into())
}

/// The lowest price at which SELLING the excess Kalshi YES still beats what we
/// paid for it by `MIN_LOCK` a contract, net of the taker fee — Case B.
///
/// The owner's standing instruction is that small naked positions should be
/// opportunistically closed PROFITABLY, and the second half of that is the
/// operative half: this never dumps at the touch to flatten. `p - fee(p)` is
/// strictly increasing (derivative `1 - 0.07·(1-2p)`, positive everywhere), so
/// the smallest tick clearing
///
/// ```text
///   L - fee(L) >= basis + MIN_LOCK
/// ```
///
/// is the whole answer, and any fill at or above it is at least as good. Sending
/// the FLOOR rather than the bid is what makes that true of a book that moves
/// between the read and the fill: a limit sell at the floor cannot trade below
/// it, where a limit at the bid could.
pub fn sell_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    ladder: &[(String, String, String)],
    basis: D,
    qty: i64,
) -> Result<D, String> {
    let lock = cx.parse_exact(MIN_LOCK);
    let want = cx.add(basis, lock);
    let Some(mut l_tick) = ceil_to_tick(cx, ladder, want) else {
        return Err(format!(
            "{} is outside every rung of the venue's tick ladder, so it has no legal \
             spelling",
            cx.emit_6dp(want)
        ));
    };
    for _ in 0..MAX_TICK_WALK {
        let fee = fee_per_ct(cx, fees, l_tick, qty);
        let net = cx.sub(l_tick, fee);
        if cx.cmp(net, want) != Ordering::Less {
            return Ok(l_tick);
        }
        let Some(next) = tick_up(cx, ladder, l_tick) else {
            return Err(format!(
                "no price on the ladder nets {} after the taker fee — the position cannot be \
                 closed at a profit at any legal price",
                cx.emit_6dp(want)
            ));
        };
        l_tick = next;
        if cx.cmp(l_tick, cx.one) != Ordering::Less {
            return Err(format!(
                "closing this profitably would need a price at or above $1 against a basis \
                 of {} — it cannot be done",
                cx.emit_6dp(basis)
            ));
        }
    }
    Err("the limit search did not converge — refusing rather than guessing".into())
}

// --------------------------------------------------------------- the decision ---

/// What one confirmed finding turns into.
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub market: String,
    /// `Bid` buys YES (Case A), `Ask` sells YES (Case B) — the raw venue side.
    pub buy: bool,
    pub qty: i64,
    /// 4dp, the way the wire wants it.
    pub limit: String,
    /// The ledger cost per contract this was priced against, carried so the log
    /// line and the booked record both name the number the decision rested on.
    pub basis: String,
    /// The open record that vouched for `basis`.
    pub lot_ts: f64,
}

/// Decide what to do about one CONFIRMED finding, given a quote and the ledger.
///
/// Pure. Every refusal is a `String` a human can act on, because at 3am the
/// difference between "the book is not paying" and "we hold a position our own
/// books have never heard of" is the whole message.
#[allow(clippy::too_many_arguments)]
pub fn decide(
    cx: &mut Cx,
    fees: &FeeSchedule,
    records: &[Value],
    f: &crate::positions::Finding,
    quote: &Quote,
    now: f64,
) -> Result<Order, String> {
    if quote.status != "active" {
        return Err(format!(
            "Kalshi reports {} as `{}`, not `active` — a market that is not trading takes no \
             order, whatever the book says",
            f.kalshi, quote.status
        ));
    }
    let contested = contested_by_other_author(records, &f.rel_id, now);
    if !contested.is_empty() {
        return Err(format!(
            "another writer booked an OPEN basket on {} at ts {contested:?}, inside the {}s \
             contest window (which is arbbot-hedge.timer's own period) — this leg may already \
             have been completed by the timer and the venue read not caught up",
            f.rel_id,
            ledger::CONTEST_WINDOW_S
        ));
    }
    inflight_check(&f.kalshi)?;

    match f.leg {
        crate::positions::Leg::PmShort => {
            let lot = worst_lot(
                cx,
                fees,
                records,
                &f.rel_id,
                Venue::PolymarketUs,
                &f.pmus,
                Held::ShortYes,
            )?;
            let qty = f.qty.min(lot.qty).min(MAX_CLIP);
            if qty < 1 {
                return Err(format!(
                    "nothing to buy: naked {} against a vouched lot of {}",
                    f.qty, lot.qty
                ));
            }
            let Some(ask) = quote.yes_ask.as_deref() else {
                return Err(format!(
                    "{} has no ask — the book offers nothing to lift, so there is no hedge to \
                     place (the venue spells this `0.0000`, which the Python reads as a price \
                     of zero)",
                    f.kalshi
                ));
            };
            let Some(ask_d) = cx.parse(ask) else {
                return Err(format!("{} quoted an unparseable ask {ask:?}", f.kalshi));
            };
            let basis = cx
                .parse(&lot.cost_per_ct)
                .ok_or_else(|| format!("unparseable ledger basis {}", lot.cost_per_ct))?;
            let limit = buy_limit(cx, fees, &quote.ladder, basis, qty)?;
            if cx.cmp(ask_d, limit) == Ordering::Greater {
                return Err(format!(
                    "ask {ask} is above the profit limit {} (ledger basis {}/ct from the open \
                     record at ts {}) — waiting for a better book, which is the policy",
                    cx.emit_6dp(limit),
                    lot.cost_per_ct,
                    lot.open_ts
                ));
            }
            let limit = cx.quantize_4dp(limit);
            Ok(Order {
                market: f.kalshi.clone(),
                buy: true,
                qty,
                limit: limit.to_standard_notation_string(),
                basis: lot.cost_per_ct,
                lot_ts: lot.open_ts,
            })
        }
        crate::positions::Leg::KalshiLong => {
            let lot =
                worst_lot(cx, fees, records, &f.rel_id, Venue::Kalshi, &f.kalshi, Held::LongYes)?;
            let qty = f.qty.min(lot.qty).min(MAX_CLIP);
            if qty < 1 {
                return Err(format!(
                    "nothing to sell: excess {} against a vouched lot of {}",
                    f.qty, lot.qty
                ));
            }
            let Some(bid) = quote.yes_bid.as_deref() else {
                return Err(format!(
                    "{} has no bid — nothing is offering to buy these back, so there is no \
                     profitable close to make",
                    f.kalshi
                ));
            };
            let Some(bid_d) = cx.parse(bid) else {
                return Err(format!("{} quoted an unparseable bid {bid:?}", f.kalshi));
            };
            let basis = cx
                .parse(&lot.cost_per_ct)
                .ok_or_else(|| format!("unparseable ledger basis {}", lot.cost_per_ct))?;
            let limit = sell_limit(cx, fees, &quote.ladder, basis, qty)?;
            if cx.cmp(bid_d, limit) == Ordering::Less {
                return Err(format!(
                    "bid {bid} is below the profit floor {} (ledger basis {}/ct from the open \
                     record at ts {}) — HOLDING. Selling here would realise a loss to flatten, \
                     and flattening is not the instruction",
                    cx.emit_6dp(limit),
                    lot.cost_per_ct,
                    lot.open_ts
                ));
            }
            let limit = cx.quantize_4dp(limit);
            Ok(Order {
                market: f.kalshi.clone(),
                buy: false,
                qty,
                limit: limit.to_standard_notation_string(),
                basis: lot.cost_per_ct,
                lot_ts: lot.open_ts,
            })
        }
    }
}

// ----------------------------------------------------------------- the record ---

/// The ledger record a filled Case A order writes: a NEW complete basket.
///
/// Deliberately NOT fee-complete, and it says so with `fees_pending`, exactly as
/// `engine::fill::book_basket` does and for the same reason — the fill report
/// that carries the venue's own fee is not read here. The PM leg is recorded at
/// the price the LEDGER vouched for, not at anything the venue said, because
/// that is the number this trade was actually decided on.
pub fn basket_record(f: &crate::positions::Finding, o: &Order, filled: i64, ts: f64) -> Value {
    serde_json::json!({
        "ts": ts,
        "relationship_id": f.rel_id,
        "title": format!("{} (rust naked-leg hedge)", f.rel_id),
        "qty": filled,
        "strategy": "take-take",
        "status": "open",
        "source": ledger::SOURCE,
        "fees_pending": true,
        "naked_hedge_basis": o.basis,
        "naked_hedge_lot_ts": o.lot_ts,
        "legs": [
            {"venue": "kalshi", "market_id": o.market, "side": "yes", "role": "taker",
             "qty": filled, "yes_price": o.limit},
            {"venue": "polymarket_us", "market_id": f.pmus, "side": "no", "role": "taker",
             "qty": filled, "yes_price": o.basis},
        ],
    })
}

/// The ledger record a filled Case B order writes: a partial unwind of the lot
/// whose basis it beat.
///
/// # What this record does NOT claim
///
/// It does not claim a realized P&L, and `pnl_pending` says so. Only the KALSHI
/// leg traded — the PM-US side of that basket is already gone at the venue,
/// which is why the Kalshi long was in excess in the first place — so the
/// proceeds here cover one leg of a two-leg record and any `realized_pnl_usd`
/// computed from them would be a confident wrong number. `unwind.rs` documents
/// the same asymmetry from the other end; `engine::fill::book_basket` sets the
/// precedent for naming the gap in the record rather than filling it with a
/// guess.
pub fn close_record(f: &crate::positions::Finding, o: &Order, filled: i64, ts: f64) -> Value {
    serde_json::json!({
        "ts": ts,
        "relationship_id": f.rel_id,
        "title": format!("{} (rust naked-long close)", f.rel_id),
        "strategy": "naked-close",
        "status": "unwound",
        "closes_ts": o.lot_ts,
        "qty": filled,
        "source": ledger::SOURCE,
        "pnl_pending": true,
        "naked_close_basis": o.basis,
        "note": "venue-truth reconciliation found this Kalshi YES long in excess of the \
                 PM-US short it was booked against; the excess was sold above its ledger \
                 basis net of the taker fee. ONLY the Kalshi leg traded here, so \
                 proceeds_usd/realized_pnl_usd are deliberately absent — the PM-US leg of \
                 the record this closes was already gone at the venue and its exit price is \
                 not known to this process. RECONCILE BY HAND.",
        "legs": [
            {"venue": "kalshi", "market_id": o.market, "side": "yes", "action": "sell",
             "role": "taker", "qty": filled, "yes_price": o.limit},
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::positions::{Finding, Leg};

    fn v(s: &str) -> Value {
        serde_json::from_str(s).expect("fixture")
    }

    /// Kalshi's tapered ladder, verbatim from `data/catalogs/kalshi-standalone.json`
    /// (`KXTIMEDECADE20S-30-VZEL` and 7,223 others).
    fn tapered() -> Vec<(String, String, String)> {
        vec![
            ("0.0000".into(), "0.1000".into(), "0.0010".into()),
            ("0.1000".into(), "0.9000".into(), "0.0100".into()),
            ("0.9000".into(), "1.0000".into(), "0.0010".into()),
        ]
    }

    /// The penny ladder, the commonest shape (26,191 of those rows).
    fn penny() -> Vec<(String, String, String)> {
        vec![("0.0000".into(), "1.0000".into(), "0.0100".into())]
    }

    fn quote(ladder: Vec<(String, String, String)>, bid: Option<&str>, ask: Option<&str>) -> Quote {
        Quote {
            market: "K-a".into(),
            status: "active".into(),
            yes_bid: bid.map(str::to_string),
            yes_ask: ask.map(str::to_string),
            ladder,
        }
    }

    fn finding(leg: Leg, qty: i64) -> Finding {
        Finding {
            rel_id: "r1".into(),
            kalshi: "K-a".into(),
            pmus: "p-a".into(),
            kq: 0.0,
            pq: 0.0,
            imb: 0.0,
            leg,
            qty,
        }
    }

    /// An ordinary open basket: PM-US short YES at 0.22 (so 0.78 a contract of
    /// NO), Kalshi long YES at 0.19. The exact shape of the 42 `cost`+`yes_price`
    /// legs in the live ledger.
    fn open_basket(ts: f64, qty: i64, pm_yes: &str, k_yes: &str) -> Value {
        v(&format!(
            r#"{{"ts":{ts},"relationship_id":"r1","status":"open","qty":{qty},"legs":[
                 {{"venue":"polymarket_us","market_id":"p-a","side":"no","role":"maker",
                   "qty":{qty},"yes_price":"{pm_yes}"}},
                 {{"venue":"kalshi","market_id":"K-a","side":"yes","role":"taker",
                   "qty":{qty},"yes_price":"{k_yes}"}}]}}"#
        ))
    }

    fn ready() -> (Cx, FeeSchedule) {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        (cx, fees)
    }

    /// Nothing may place unless the engine has said what it is working. This is
    /// the fail-closed default, and every decide test below has to clear it —
    /// under [`TEST_SERIAL`], which is shared with `positions`' act tests.
    async fn allow_all() -> tokio::sync::MutexGuard<'static, ()> {
        let g = TEST_SERIAL.lock().await;
        publish_inflight(BTreeSet::new());
        g
    }

    // ---- the ladder -------------------------------------------------------

    /// THE PYTHON'S TICK BUG, stated as a test. `kalshi_ask` returns the first
    /// rung's step (0.0010) and would price a $0.42 order to the tenth of a
    /// cent; the venue's tick there is a full cent.
    #[test]
    fn a_tapered_ladder_uses_the_rung_the_price_is_in() {
        let (mut cx, _) = ready();
        let l = tapered();
        let x = cx.parse_exact("0.4267");
        let got = floor_to_tick(&mut cx, &l, x).expect("inside the ladder");
        assert_eq!(cx.emit_6dp(got), "0.420000", "a cent tick, not a tenth of one");
        // ...and below $0.10 the same ladder really is deci-cent.
        let x = cx.parse_exact("0.0567");
        let got = floor_to_tick(&mut cx, &l, x).expect("inside the ladder");
        assert_eq!(cx.emit_6dp(got), "0.056000");
    }

    /// A step DOWN out of the cent rung lands on the TOP of the deci-cent rung
    /// below it — $0.0990 — not on $0.09. Subtracting the departing rung's own
    /// cent tick would skip nine legal prices, and each skipped price is edge
    /// given away for nothing.
    #[test]
    fn a_tick_step_crosses_a_rung_boundary_without_skipping_prices() {
        let (mut cx, _) = ready();
        let l = tapered();
        let x = cx.parse_exact("0.1000");
        let down = tick_down(&mut cx, &l, x).expect("a price below 0.10 exists");
        assert_eq!(cx.emit_6dp(down), "0.099000");
        let up = tick_up(&mut cx, &l, down).expect("and back up again");
        assert_eq!(cx.emit_6dp(up), "0.100000", "the boundary is reachable from below");
        // ...and inside a rung it is the plain step.
        let mid = cx.parse_exact("0.4200");
        let down = tick_down(&mut cx, &l, mid).expect("inside the cent rung");
        assert_eq!(cx.emit_6dp(down), "0.410000");
    }

    /// The bottom of the ladder has nothing below it, and that is `None` rather
    /// than a negative price.
    #[test]
    fn the_bottom_of_the_ladder_has_no_next_price_down() {
        let (mut cx, _) = ready();
        let l = tapered();
        let x = cx.parse_exact("0.0000");
        assert!(tick_down(&mut cx, &l, x).is_none());
    }

    /// A price off the end of the ladder has no legal spelling, and is refused
    /// rather than clamped into one.
    #[test]
    fn a_price_outside_every_rung_has_no_tick() {
        let (mut cx, _) = ready();
        let l = penny();
        let x = cx.parse_exact("1.5000");
        assert!(floor_to_tick(&mut cx, &l, x).is_none());
        let x = cx.parse_exact("-0.0100");
        assert!(floor_to_tick(&mut cx, &l, x).is_none());
    }

    // ---- the basis --------------------------------------------------------

    /// The PM-US leg's cost is what is LEFT of the dollar, plus its fee. A
    /// maker leg at yes 0.22 costs 0.78; PM-US charges makers nothing.
    #[test]
    fn a_pm_short_lot_costs_one_minus_the_yes_price() {
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let lot = worst_lot(
            &mut cx,
            &fees,
            &recs,
            "r1",
            Venue::PolymarketUs,
            "p-a",
            Held::ShortYes,
        )
        .expect("one open lot");
        assert_eq!(lot.cost_per_ct, "0.780000");
        assert_eq!(lot.qty, 5);
        assert_eq!(lot.open_ts, 1.0);
    }

    /// THE REFUSAL THIS MODULE EXISTS FOR. `xvus-fedcut-26-usfed-2026-cut` holds
    /// a real PM-US short and has NO open record in the live ledger — every one
    /// of its baskets is unwound. The Python priced that leg off `costPerShare`
    /// and got 9.4; this must decline, by name.
    #[test]
    fn a_position_with_no_open_record_yields_no_basis_at_all() {
        let (mut cx, fees) = ready();
        // an unwound basket and its correction: the exact shape of that history
        let recs = vec![
            open_basket(1.0, 5, "0.22", "0.19"),
            v(r#"{"ts":2.0,"relationship_id":"r1","status":"unwound","closes_ts":1.0,"qty":5}"#),
        ];
        let e = worst_lot(
            &mut cx,
            &fees,
            &recs,
            "r1",
            Venue::PolymarketUs,
            "p-a",
            Held::ShortYes,
        )
        .expect_err("a fully unwound relationship vouches for nothing");
        assert!(e.contains("unaccounted for"), "{e}");
        assert!(e.contains("none will be invented"), "{e}");
    }

    /// An INVERTED basket (long PM YES against short Kalshi) is 9 of the open
    /// records in the live file. Its PM leg is on the right market pointing the
    /// wrong way, and completing it is not the trade this is.
    #[test]
    fn a_lot_pointing_the_other_way_is_not_a_basis() {
        let (mut cx, fees) = ready();
        let recs = vec![v(
            r#"{"ts":1.0,"relationship_id":"r1","status":"open","qty":5,"legs":[
                 {"venue":"polymarket_us","market_id":"p-a","side":"yes","role":"taker",
                  "qty":5,"yes_price":"0.22"},
                 {"venue":"kalshi","market_id":"K-a","side":"no","role":"taker",
                  "qty":5,"yes_price":"0.19"}]}"#,
        )];
        let e = worst_lot(
            &mut cx,
            &fees,
            &recs,
            "r1",
            Venue::PolymarketUs,
            "p-a",
            Held::ShortYes,
        )
        .expect_err("an inverted basket is not a naked short's other half");
        assert!(e.contains("1 pointing the other way"), "{e}");
    }

    /// Two open lots, and the DEARER one prices the hedge. Contracts are
    /// fungible, so the cheap lot cannot be the one we assume we are completing.
    #[test]
    fn the_dearest_lot_is_the_basis() {
        let (mut cx, fees) = ready();
        let recs = vec![
            open_basket(1.0, 5, "0.30", "0.19"), // NO cost 0.70
            open_basket(2.0, 5, "0.22", "0.19"), // NO cost 0.78 — dearer
        ];
        let lot = worst_lot(
            &mut cx,
            &fees,
            &recs,
            "r1",
            Venue::PolymarketUs,
            "p-a",
            Held::ShortYes,
        )
        .expect("two lots");
        assert_eq!(lot.cost_per_ct, "0.780000");
        assert_eq!(lot.open_ts, 2.0, "and it names the record it came from");
    }

    /// The Kalshi side, with the venue's OWN reported fee — the
    /// `avg_price`+`fees` shape that 40 legs in the live file carry. 0.19 plus
    /// 0.4417/41 of fee.
    #[test]
    fn a_kalshi_lot_uses_the_fee_the_venue_reported() {
        let (mut cx, fees) = ready();
        let recs = vec![v(
            r#"{"ts":1.0,"relationship_id":"r1","status":"open","qty":41,"legs":[
                 {"venue":"kalshi","market_id":"K-a","side":"yes","role":"taker","qty":41,
                  "avg_price":"0.1900","fees":"0.4417"},
                 {"venue":"polymarket_us","market_id":"p-a","side":"no","role":"taker",
                  "qty":41,"yes_price":"0.2400"}]}"#,
        )];
        let lot =
            worst_lot(&mut cx, &fees, &recs, "r1", Venue::Kalshi, "K-a", Held::LongYes)
                .expect("one lot");
        assert_eq!(lot.cost_per_ct, "0.200773", "0.19 + 0.4417/41");
    }

    /// A partial unwind reduces the lot; a full one removes it.
    #[test]
    fn unwinds_net_off_the_lot_they_close() {
        let (mut cx, fees) = ready();
        let recs = vec![
            open_basket(1.0, 5, "0.22", "0.19"),
            v(r#"{"ts":9.0,"relationship_id":"r1","status":"unwound","closes_ts":1.0,"qty":3}"#),
        ];
        let lot = worst_lot(
            &mut cx,
            &fees,
            &recs,
            "r1",
            Venue::PolymarketUs,
            "p-a",
            Held::ShortYes,
        )
        .expect("2 left");
        assert_eq!(lot.qty, 2);
    }

    /// A correction that rewrites the leg prices rewrites the basis with them —
    /// `apply_corrections` shallow-merges `legs` wholesale, and the live file
    /// has 45 correction records.
    #[test]
    fn a_correction_moves_the_basis() {
        let (mut cx, fees) = ready();
        let recs = vec![
            open_basket(1.0, 5, "0.22", "0.19"),
            v(r#"{"ts":3.0,"relationship_id":"r1","status":"correction","corrects_ts":1.0,
                  "fields":{"legs":[
                    {"venue":"polymarket_us","market_id":"p-a","side":"no","role":"maker",
                     "qty":5,"yes_price":"0.10"}]}}"#),
        ];
        let lot = worst_lot(
            &mut cx,
            &fees,
            &recs,
            "r1",
            Venue::PolymarketUs,
            "p-a",
            Held::ShortYes,
        )
        .expect("corrected lot");
        assert_eq!(lot.cost_per_ct, "0.900000", "1 - 0.10, not 1 - 0.22");
    }

    // ---- the prices -------------------------------------------------------

    /// The fee is charged at the LIMIT, not at the ask. Basis 0.78 leaves 0.215
    /// of headroom after MIN_LOCK; on a penny ladder the fee-free bound floors
    /// to 0.21, and 0.78 + 0.21 + fee(0.21) + 0.005 is over a dollar because a
    /// 1-lot's fee ceils to 2c — so the answer must be a tick lower.
    #[test]
    fn the_buy_limit_pays_for_the_fee_at_the_price_it_sends() {
        let (mut cx, fees) = ready();
        let basis = cx.parse_exact("0.78");
        let l = buy_limit(&mut cx, &fees, &penny(), basis, 1).expect("a price exists");
        let l_s = cx.emit_6dp(l);
        // prove the property rather than pinning a constant: the completed
        // basket, at the price we would send, costs under a dollar with the lock
        // still in it.
        let fee = fee_per_ct(&mut cx, &fees, l, 1);
        let total = cx.add(basis, l);
        let total = cx.add(total, fee);
        let lock = cx.parse_exact(MIN_LOCK);
        let total = cx.add(total, lock);
        assert!(
            cx.cmp(total, cx.one) != Ordering::Greater,
            "limit {l_s} leaves the basket at {} — over the dollar",
            cx.emit_6dp(total)
        );
        // ...and it is not needlessly low: one tick up must FAIL the same test.
        let up = tick_up(&mut cx, &penny(), l).expect("a tick above");
        let fee = fee_per_ct(&mut cx, &fees, up, 1);
        let total = cx.add(basis, up);
        let total = cx.add(total, fee);
        let total = cx.add(total, lock);
        assert!(
            cx.cmp(total, cx.one) == Ordering::Greater,
            "a tick above {l_s} should not have been affordable"
        );
    }

    /// A clip of 5 pays a smaller fee PER CONTRACT than a clip of 1, because
    /// the venue ceils to the cent once over the whole order — so sizing has to
    /// happen before pricing.
    #[test]
    fn a_bigger_clip_can_afford_a_higher_limit() {
        let (mut cx, fees) = ready();
        let basis = cx.parse_exact("0.78");
        let one = buy_limit(&mut cx, &fees, &penny(), basis, 1).expect("1-lot");
        let five = buy_limit(&mut cx, &fees, &penny(), basis, 5).expect("5-lot");
        assert!(
            cx.cmp(five, one) != Ordering::Less,
            "5-lot {} must not be priced below the 1-lot {}",
            cx.emit_6dp(five),
            cx.emit_6dp(one)
        );
    }

    /// A basket already at or over a dollar has no completing price, and saying
    /// so is different from saying the book is thin.
    #[test]
    fn a_basis_that_leaves_no_headroom_is_a_named_refusal() {
        let (mut cx, fees) = ready();
        let basis = cx.parse_exact("0.998");
        let e = buy_limit(&mut cx, &fees, &penny(), basis, 1).expect_err("no price completes it");
        assert!(e.contains("buying a known loss"), "{e}");
    }

    /// The sell floor beats the basis NET of the fee, and one tick below it does
    /// not. This is the whole of "only above our own cost basis net of fees".
    #[test]
    fn the_sell_floor_clears_the_basis_after_the_fee() {
        let (mut cx, fees) = ready();
        let basis = cx.parse_exact("0.20");
        let l = sell_limit(&mut cx, &fees, &penny(), basis, 5).expect("a price exists");
        let lock = cx.parse_exact(MIN_LOCK);
        let want = cx.add(basis, lock);
        let fee = fee_per_ct(&mut cx, &fees, l, 5);
        let net = cx.sub(l, fee);
        assert!(
            cx.cmp(net, want) != Ordering::Less,
            "floor {} nets {} against a needed {}",
            cx.emit_6dp(l),
            cx.emit_6dp(net),
            cx.emit_6dp(want)
        );
        let down = tick_down(&mut cx, &penny(), l).expect("a tick below");
        let fee = fee_per_ct(&mut cx, &fees, down, 5);
        let net = cx.sub(down, fee);
        assert!(cx.cmp(net, want) == Ordering::Less, "the floor is not one tick too high");
    }

    // ---- the decision -----------------------------------------------------

    /// Case A end to end: a naked PM short, an ask inside the limit, a basis the
    /// ledger vouches for.
    #[tokio::test]
    async fn a_cheap_ask_completes_the_basket() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let q = quote(penny(), Some("0.18"), Some("0.19"));
        let o = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e9)
            .expect("0.19 is well inside a 0.78 basis");
        assert!(o.buy);
        assert_eq!(o.qty, 3);
        assert_eq!(o.market, "K-a");
        assert_eq!(o.basis, "0.780000");
        assert_eq!(o.lot_ts, 1.0);
    }

    /// ...and a dear one waits. The refusal names the basis and the record it
    /// came from, which is what makes it auditable.
    #[tokio::test]
    async fn a_dear_ask_waits_and_says_what_it_waited_on() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let q = quote(penny(), Some("0.90"), Some("0.95"));
        let e = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e9)
            .expect_err("0.95 against a 0.78 basis is a loss");
        assert!(e.contains("waiting for a better book"), "{e}");
        assert!(e.contains("0.780000"), "names the basis: {e}");
    }

    /// THE `0.0000` TRAP. Kalshi spells an empty ask as a price of zero, and
    /// `0 <= limit` is true for every limit ever computed — so the Python would
    /// send an order into a book with nothing in it.
    #[tokio::test]
    async fn an_empty_ask_is_not_a_free_hedge() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let q = quote(penny(), Some("0.18"), None);
        let e = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e9)
            .expect_err("no ask is not an ask of zero");
        assert!(e.contains("no ask"), "{e}");
    }

    /// The clip cap binds, and it binds on the SMALLEST of the three bounds.
    #[tokio::test]
    async fn the_clip_is_capped_at_the_smallest_of_naked_lot_and_max() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 40, "0.22", "0.19")];
        let q = quote(penny(), Some("0.18"), Some("0.19"));
        let o = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 30), &q, 1e9).expect("ok");
        assert_eq!(o.qty, MAX_CLIP, "30 naked, 40 vouched, capped at the clip");

        // ...and the LOT is the binding one when it is smallest.
        let recs = vec![open_basket(1.0, 2, "0.22", "0.19")];
        let o = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 30), &q, 1e9).expect("ok");
        assert_eq!(o.qty, 2, "we only ever hedge what a ledger lot vouches for");
    }

    /// Case B: the excess Kalshi YES sells above its basis. The Python refuses
    /// this case entirely, which is why two of them have stood for 30 hours.
    #[tokio::test]
    async fn an_excess_kalshi_long_sells_above_its_basis() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let q = quote(penny(), Some("0.40"), Some("0.42"));
        let o = decide(&mut cx, &fees, &recs, &finding(Leg::KalshiLong, 2), &q, 1e9)
            .expect("0.40 against a ~0.20 basis is a profit");
        assert!(!o.buy, "this one SELLS");
        assert_eq!(o.qty, 2);
    }

    /// ...and NEVER at the touch to flatten. A bid under the basis is a hold,
    /// and the message says which of the two it is.
    #[tokio::test]
    async fn a_bid_below_the_basis_holds_rather_than_dumping() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let q = quote(penny(), Some("0.10"), Some("0.12"));
        let e = decide(&mut cx, &fees, &recs, &finding(Leg::KalshiLong, 2), &q, 1e9)
            .expect_err("0.10 against a ~0.20 basis is a realised loss");
        assert!(e.contains("HOLDING"), "{e}");
        assert!(e.contains("flattening is not the instruction"), "{e}");
    }

    /// A market the venue is not trading takes no order, whatever its last
    /// quote said. This is the pre-flight half of the halt policy — the park is
    /// the other half, and at a 5-minute cycle this is the one that bites.
    #[tokio::test]
    async fn a_market_that_is_not_active_is_refused_before_anything_is_priced() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let mut q = quote(penny(), Some("0.18"), Some("0.19"));
        q.status = "finalized".into();
        let e = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e9)
            .expect_err("a finalized market is not tradable");
        assert!(e.contains("not `active`"), "{e}");
    }

    // ---- the interlock ----------------------------------------------------

    /// THE DOUBLE HEDGE. An obligation this engine already owes on the same
    /// Kalshi market stops the order dead.
    #[tokio::test]
    async fn a_leg_this_engine_already_owes_is_never_placed_against() {
        let _g = TEST_SERIAL.lock().await;
        let (mut cx, fees) = ready();
        publish_inflight(BTreeSet::from(["K-a".to_string()]));
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let q = quote(penny(), Some("0.18"), Some("0.19"));
        let e = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e9)
            .expect_err("the engine owes a hedge here");
        assert!(e.contains("double hedge"), "{e}");
        assert!(e.contains("hedges_overfilled"), "names why it is invisible: {e}");
    }

    /// A publisher that has never spoken is not an empty set. This is the
    /// fail-closed default and it must survive somebody deleting the publish
    /// call.
    #[tokio::test]
    async fn an_unpublished_obligation_set_refuses_everything() {
        let _g = TEST_SERIAL.lock().await;
        reset_inflight();
        assert!(inflight_check("K-a").is_err());
        publish_inflight(BTreeSet::new());
        assert!(inflight_check("K-a").is_ok());
    }

    /// The frozen Python hedger having booked this relationship minutes ago is
    /// a refusal, because the venue read may not have caught up with it.
    #[tokio::test]
    async fn a_recent_basket_from_another_writer_stops_the_order() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![
            open_basket(1.0, 5, "0.22", "0.19"),
            v(r#"{"ts":999900.0,"relationship_id":"r1","status":"open","qty":1,
                  "source":"py:hedge_naked_legs","legs":[
                    {"venue":"kalshi","market_id":"K-a","side":"yes","qty":1,
                     "yes_price":"0.19"}]}"#),
        ];
        let q = quote(penny(), Some("0.18"), Some("0.19"));
        let e = decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e6)
            .expect_err("inside the contest window");
        assert!(e.contains("contest window"), "{e}");
    }

    /// ...and one OUTSIDE the window does not, or the module could never act at
    /// all on a relationship the timer has ever touched.
    #[tokio::test]
    async fn an_old_basket_from_another_writer_does_not() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![
            open_basket(1.0, 5, "0.22", "0.19"),
            v(r#"{"ts":1.0,"relationship_id":"r1","status":"open","qty":1,
                  "source":"py:hedge_naked_legs","legs":[
                    {"venue":"kalshi","market_id":"K-a","side":"yes","qty":1,
                     "yes_price":"0.19"}]}"#),
        ];
        let q = quote(penny(), Some("0.18"), Some("0.19"));
        assert!(decide(&mut cx, &fees, &recs, &finding(Leg::PmShort, 3), &q, 1e9).is_ok());
    }

    // ---- the records ------------------------------------------------------

    /// A Case A record is a complete basket that `open_exposure` will count and
    /// `ledger::append_basket` will police — including the `source` stamp the
    /// single-author rule reads off.
    #[test]
    fn a_completed_basket_is_booked_as_this_engines_own() {
        let f = finding(Leg::PmShort, 3);
        let o = Order {
            market: "K-a".into(),
            buy: true,
            qty: 3,
            limit: "0.1900".into(),
            basis: "0.780000".into(),
            lot_ts: 1.0,
        };
        let rec = basket_record(&f, &o, 3, 12.0);
        assert_eq!(rec["status"], "open");
        assert_eq!(rec["source"], ledger::SOURCE);
        assert_eq!(rec["qty"], 3);
        assert_eq!(rec["legs"][0]["yes_price"], "0.1900");
        assert_eq!(rec["legs"][1]["yes_price"], "0.780000", "the LEDGER basis, not a venue read");
        assert_eq!(
            crate::ledger::open_exposure(vec![rec]).get("r1").copied(),
            Some(3.0),
            "the next restart must seed this exposure"
        );
    }

    /// A Case B record CLOSES the lot it beat, and refuses to state a P&L it
    /// only knows half of.
    #[test]
    fn a_naked_long_close_names_its_lot_and_claims_no_pnl() {
        let f = finding(Leg::KalshiLong, 2);
        let o = Order {
            market: "K-a".into(),
            buy: false,
            qty: 2,
            limit: "0.4000".into(),
            basis: "0.200773".into(),
            lot_ts: 1.0,
        };
        let rec = close_record(&f, &o, 2, 12.0);
        assert_eq!(rec["status"], "unwound");
        assert_eq!(rec["closes_ts"], 1.0);
        assert_eq!(rec["pnl_pending"], true);
        assert!(rec.get("realized_pnl_usd").is_none(), "it does not know this");
        assert!(rec.get("proceeds_usd").is_none(), "nor this");
        // and it really does reduce the open exposure the caps read
        let open = open_basket(1.0, 5, "0.22", "0.19");
        assert_eq!(
            crate::ledger::open_exposure(vec![open, rec]).get("r1").copied(),
            Some(3.0)
        );
    }
}
