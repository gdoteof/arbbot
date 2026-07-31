//! Marking open baskets to the LIVE BOOK, in-process.
//!
//! Port of `scripts/mark_positions.py`, which `arbbot-marks.timer` ran every
//! two minutes: it re-read `data/exec/trades.jsonl`, fetched top-of-book for
//! every open basket over REST, and rewrote `data/exec/marks.json`.
//!
//! **THIS IS NOW THE ONLY WRITER.** `arbbot-marks.timer` was stopped and
//! disabled 2026-07-31; the armed engine runs with `--marks-out
//! data/exec/marks.json` and writes the file it also reads its take-take bar
//! from. Parity was proved first, from the dry-run shadow beside the live timer:
//! 25 of 25 rows, identical row set AND order, 14 of 15 numeric fields
//! bit-identical, the 15th (`unwind_apr`) differing only by the write gap it is
//! measured from. Both wrote ZERO unpriced rows.
//!
//! THE ENGINE ALREADY HAS THE BOOK. It holds a live `BookBuilder` fed by the
//! recorder's socket, so a mark it computes is current as of the last event on
//! that market — not up to 120 seconds old, and not a second round trip to two
//! venues. That is the whole reason this exists; the arithmetic is unchanged.
//!
//! # The output is a CONTRACT, not a convenience
//!
//! `data/exec/marks.json` has consumers in three processes, and none of them
//! may be able to tell which one wrote it:
//!
//!   * **this engine**, twice: [`crate::taketake::blended_apr`] derives the
//!     take-take bar from `cost_usd` / `locked_profit_usd` / `resolves_by`, and
//!     [`crate::unwind::select`] decides from `forward_hold_apr` /
//!     `maker_exit_ct` / `resolves_by` / `qty` / `ts` / `kalshi_ticker`;
//!   * `src/arbbot/dash/data.py`, which reads eleven per-position fields
//!     including `natural_hold_apr` — a field only the dashboard reads. It is
//!     emitted anyway: "schema compatible" is decided by the consumers, not by
//!     the list of things worth porting;
//!   * operator monitors, which read the whole `totals` block.
//!
//! So the field set, the types and the `totals` keys are pinned by
//! [`tests::the_schema_matches_the_python_writer_field_for_field`] against a row
//! taken verbatim from the live file, and `totals.n_open` deliberately does NOT
//! equal `positions.len()` — see [`Totals::n_open`].
//!
//! # What this does NOT do
//!
//! It does not place a maker exit. `maker_exit_ct` / `maker_exit_eligible` are
//! computed and published exactly as the Python writes them, and that is the
//! seam: a placer consumes those two fields (or [`crate::unwind::select`], which
//! reads the same file and applies its own stricter floor) and nothing here has
//! to change to serve it. Deciding is here; acting is not.
//!
//! It also does not write `data/exec/marks_history.jsonl`. `mark_positions.py`
//! appends one line per run there and the dashboard's Exits view reads it; at
//! this module's re-mark rate that file would grow about a hundred times
//! faster, so the port stops at the snapshot. **Retiring `arbbot-marks.timer`
//! therefore stopped the Exits history** — that happened on 2026-07-31, so the
//! dashboard's Exits view now has a file that ends rather than one that grows.
//! Known and accepted, not a defect here; restoring it wants a rate-limited
//! appender, not a re-mark-rate one.
//!
//! # Deliberate divergences from the Python
//!
//! Each of these is a place where copying the Python exactly would be worse.
//! They are listed together because a reader diffing the two files needs to know
//! which differences are intentional.
//!
//! 1. **An empty book side is `None`, not `0`.** `mark_positions.py:70-71` reads
//!    `Decimal(str(m.get("yes_bid_dollars") or 0))`, so a Kalshi market the REST
//!    call answers with a null quote becomes `Decimal("0")` — which is not
//!    `None`. The row then passes the `k_ask is not None` test, computes
//!    `k_exit_px = 0 - 0.01`, hands `-0.01` to `leg_fee`, and `curves.py:85`
//!    raises `ValueError` out of an unguarded loop, killing the run BEFORE the
//!    file is written. [`crate::unwind::Skip::NotPriceable`] documents that at
//!    length as unfixable in frozen Python. Here an absent touch is simply
//!    absent, which routes the row down the same unpriced branch a dark PM leg
//!    already takes. It is the conservative direction — a null row can never
//!    manufacture an exit signal — and it closes the hole.
//! 2. **`k_exit_px < 0` or `p_close_px > 1` refuses the maker-exit branch.**
//!    The same defect one step further in: `arb_core::fees` has no `[0,1]`
//!    domain check (Python's has one, by raising), so an out-of-band price here
//!    would compute a plausible-looking negative fee instead of stopping. A
//!    named refusal — `maker_exit_ct: null` — is the only honest answer.
//! 3. **Dates are UTC.** `_years_to` uses `datetime.date.today()` and the sports
//!    fallback uses `date.fromtimestamp`, both LOCAL. Everything else in this
//!    workspace that touches a resolve date is UTC
//!    (`arb_core::resolve::today_iso`, `taketake::blended_apr`,
//!    `unwind::Skip::Settled`), and `crate::unwind`'s header already records the
//!    four-hour window each day in which the two disagree. One clock, and it is
//!    the one the rest of the stack already uses.
//! 4. **A two-leg basket missing either venue is skipped and COUNTED as
//!    `unpriced_positions`.** `mark_positions.py:283-284` uses bare `next(...)`,
//!    which raises `StopIteration` and kills the run. Counting keeps the
//!    identity `n_open - positions.len() - unpriced_positions == single-leg
//!    records` true in both implementations, which is the only thing a monitor
//!    can check.
//!
//! # The one hazard NOT diverged from
//!
//! `inverted` is `kleg["side"] == "no"`, verbatim. Two writers spell leg sides
//! differently — the Python hedger records the POSITION (`kalshi side=yes`),
//! this engine records the ORDER (`kalshi side=bid`) — so a Kalshi maker ASK
//! fill booked by the engine reads as `side="ask"`, which is not `"no"`, and is
//! therefore marked as a standard basket when it is really an inverted one.
//! That is latent rather than live: all six engine-written records in today's
//! ledger carry no `cost_usd` and are skipped as unpriced before they get here.
//! It is left alone because changing it would change live unwind signals, and
//! this change is a port.

use arb_core::book::BookBuilder;
use arb_core::fees::{leg_fee, FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::resolve::{iso_from_day, parse_iso, resolve_date, today_iso};
use arb_core::scan::{Cx, D};
use serde::Serialize;
use serde_json::Value;
use std::cmp::Ordering;

/// Opportunity cost of locked capital, %/yr. `mark_positions.py`'s
/// `HURDLE_APR`: a hold whose REMAINING return annualizes below this is a worse
/// use of capital than fresh riskless arb (~15-18%/yr), so it is flagged for
/// unwind even before full convergence.
const HURDLE_APR: f64 = 12.0;

/// Hard floor, %/yr. Below this the hold is worse than trivially-redeployable
/// yield, so it is flagged EVEN INTO IDLE CAPITAL — the soft hurdle above stays
/// gated on being capital-constrained; this one is unconditional.
const HARD_FLOOR_APR: f64 = 4.0;

/// Seconds in a Julian year — `mark_positions.py`'s divisor.
const YEAR_S: f64 = 31_557_600.0;

/// Minimum holding period an `unwind_apr` is annualized over, so a just-opened
/// position does not annualize to infinity.
const MIN_HELD_S: f64 = 3600.0;

/// One tick, both legs. Kalshi quotes whole cents and `mark_positions.py` steps
/// PM by `Decimal("0.01")` on both sides of the exit.
const TICK: &str = "0.01";

/// Per-contract profit the maker exit must show to be `maker_exit_eligible` —
/// `mark_positions.py`'s `mx >= Decimal("0.005")`.
///
/// NOT `crate::unwind::MIN_EXIT_CT`, which is two ticks and is that module's
/// own, stricter screen against a stale snapshot. This is the writer's flag and
/// the writer's threshold; the two are deliberately different numbers, and
/// substituting one for the other here would silently move a published field.
const MIN_MAKER_EXIT_CT: &str = "0.005";

/// Edge, in probability, at which the OPPOSITE basket is reported as its own
/// fresh arb — `mark_positions.py`'s `rev >= Decimal("0.03")`.
const REVERSE_EDGE: &str = "0.03";

/// One marked basket. Field order IS the emitted key order, and it is
/// `mark_positions.py`'s insertion order: a `serde_json::Map` would sort keys
/// alphabetically, which is not wrong but makes a diff against the live file
/// unreadable.
///
/// The two `maker_exit_*` fields are `Option`-and-skipped because the Python
/// omits them entirely on an unpriced row (`mark_positions.py:251-254` sets
/// neither), and a consumer that distinguishes "absent" from "null" would see
/// the difference. The doubled `Option` on `maker_exit_ct` is that distinction:
/// the outer one is presence, the inner one is the Python's own `None`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Row {
    pub relationship_id: String,
    /// The `ts` of the `open` ledger record. Together with `relationship_id`
    /// this is the BASKET identity that the dashboard and `unwind::select` both
    /// key on: one relationship holds several independently-priced positions,
    /// entered at different prices and converging at different rates.
    pub ts: Option<f64>,
    pub title: Option<String>,
    pub kalshi_ticker: Option<String>,
    pub qty: i64,
    pub cost_usd: f64,
    pub locked_profit_usd: f64,
    pub resolves_by: Option<String>,
    pub resolves_estimated: bool,
    pub liq_value_usd: Option<f64>,
    pub mark_pnl_usd: Option<f64>,
    pub converged_pct: Option<f64>,
    pub forward_hold_apr: Option<f64>,
    pub natural_hold_apr: Option<f64>,
    pub unwind_apr: Option<f64>,
    pub unwind_signal: bool,
    pub unwind_hard: bool,
    pub reverse_edge_c: Option<f64>,
    pub reverse_signal: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_exit_ct: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_exit_eligible: Option<bool>,
}

/// The portfolio block. Every operator monitor reads it; the key set is frozen.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Totals {
    pub cost_usd: f64,
    pub mark_pnl_usd: f64,
    pub locked_profit_usd: f64,
    /// EVERY open ledger record, including the ones no row was emitted for.
    ///
    /// **This is not `positions.len()`, and must never be "fixed" to be.** On
    /// the 2026-07-31 live file it is 32 against 25 rows: one single-leg record
    /// (a directional pm-lean rider, which has no basket to mark) and six with
    /// no cost basis. The gap is the point — it is how an operator sees that the
    /// book is bigger than the marked book.
    pub n_open: u64,
    /// Open baskets that could not be marked because the record carries no
    /// `cost_usd` / `profit_usd`.
    ///
    /// COUNTED, NOT SILENTLY DROPPED, and that is load-bearing. This engine
    /// books a basket at place time and never reads fill reports, so its records
    /// carry no cost basis; marking one would be a guess. On 2026-07-28 the
    /// engine's first four live records hit `mark_positions.py`'s hard subscript
    /// and killed the timer 8 seconds after the last good run — and because this
    /// file is where the engine re-derives its own take-take bar, that froze the
    /// bar at a stale value the armed engine went on trading against for four
    /// hours.
    pub unpriced_positions: u64,
    pub unwind_signals: u64,
    pub unwind_hard: u64,
    pub reverse_signals: u64,
}

/// The whole file.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Doc {
    /// `YYYY-MM-DDTHH:MM:SSZ` — the one shape `taketake::bar_from_marks`
    /// accepts, pinned strictly there so a format change reads as untrusted
    /// rather than as a guess.
    pub generated_at: String,
    pub positions: Vec<Row>,
    pub totals: Totals,
}

impl Doc {
    /// The bytes `mark_positions.py` writes: `json.dumps(doc, indent=0)` — a
    /// newline between every item, no indentation, `": "` after every key.
    pub fn to_json(&self) -> String {
        let mut buf = Vec::new();
        let fmt = serde_json::ser::PrettyFormatter::with_indent(b"");
        let mut ser = serde_json::Serializer::with_formatter(&mut buf, fmt);
        // A `Doc` is plain data with no map keys and no non-finite floats that
        // could fail; these two are unreachable and say so.
        self.serialize(&mut ser).expect("a Doc always serializes");
        String::from_utf8(buf).expect("serde_json emits utf-8")
    }
}

/// `time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())` for epoch seconds.
fn stamp(now: f64) -> String {
    let day = (now / 86400.0).floor();
    let rem = (now - day * 86400.0) as i64;
    format!(
        "{}T{:02}:{:02}:{:02}Z",
        iso_from_day(day as i64),
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    )
}

/// `round(x, n)` with Python's semantics — round-half-to-even on the exact
/// binary value, which is what both `round()` and Rust's `{:.n$}` do.
fn round_to(x: f64, places: usize) -> f64 {
    format!("{x:.places$}").parse().unwrap_or(x)
}

/// `D` -> `f64` at the emit boundary, the same place Python calls `float()`.
/// Standard notation, so a small value cannot arrive as `1E-7`.
fn f(v: D) -> f64 {
    v.to_standard_notation_string().parse().unwrap_or(0.0)
}

fn str_of<'a>(r: &'a Value, k: &str) -> Option<&'a str> {
    r.get(k).and_then(|v| v.as_str())
}

fn f64_of(r: &Value, k: &str) -> Option<f64> {
    r.get(k).and_then(|v| v.as_f64())
}

/// The exact decimal text of a JSON number, so `Cx` sees what Python's
/// `Decimal(str(...))` saw. `to_string()` on a `Number` round-trips the literal,
/// because serde_json is built with `float_roundtrip` (see the workspace
/// manifest).
fn dec_of(cx: &mut Cx, r: &Value, k: &str) -> Option<D> {
    let n = r.get(k)?;
    if !n.is_number() {
        return None;
    }
    cx.parse(&n.to_string())
}

/// Top of book as `(best bid, best ask)`, each `None` when that side is empty
/// or the engine holds no book for the market at all.
///
/// See divergence 1 in the module header: the Python turns a missing Kalshi
/// quote into `Decimal("0")` and dies downstream on it. An absent touch is
/// absent.
fn top(cx: &mut Cx, books: &BookBuilder, venue: Venue, market: &str) -> (Option<D>, Option<D>) {
    let Some(b) = books.get(venue, market) else { return (None, None) };
    (
        b.bids.first().and_then(|l| cx.parse(&l.price)),
        b.asks.first().and_then(|l| cx.parse(&l.price)),
    )
}

/// Taker fees to liquidate both legs at the given per-leg YES exit prices.
///
/// Both venues' taker curves are `coef * P * (1-P) * size` — symmetric in `P`
/// against `1-P` — so each leg's YES price prices its fee whichever side we
/// close. This replaces a flat $0.01/ct Kalshi-only assumption that understated
/// the real round trip by 1.3-3.3x (card b83b0449).
///
/// PM-US uses the schedule's fallback taker coefficient: the venue-reported
/// per-market `feeCoefficient` is not in hand on the exit path, and the fallback
/// is deliberately conservative — it can over-state fees, never manufacture a
/// profitable-looking exit.
fn exit_fees_usd(cx: &mut Cx, fees: &FeeSchedule, k_yes: D, p_yes: D, qty: D) -> D {
    let k = leg_fee(cx, fees, Venue::Kalshi, Role::Taker, k_yes, qty, "default", None);
    let p = leg_fee(cx, fees, Venue::PolymarketUs, Role::Taker, p_yes, qty, "default", None);
    cx.add(k, p)
}

/// Fees on the OPPORTUNISTIC exit: the Kalshi leg rests as a maker ask, the PM
/// NO leg closes taker-side on fill. Kalshi charges makers (0.0175 coef) — only
/// Polymarket makers are free — so a passive exit is cheaper than the taker
/// round trip, not free.
fn maker_exit_fees_usd(cx: &mut Cx, fees: &FeeSchedule, k_yes: D, p_yes: D, qty: D) -> D {
    let k = leg_fee(cx, fees, Venue::Kalshi, Role::Maker, k_yes, qty, "default", None);
    let p = leg_fee(cx, fees, Venue::PolymarketUs, Role::Taker, p_yes, qty, "default", None);
    cx.add(k, p)
}

/// Years from `today` (UTC) to an ISO date, floored at one day so a same-day or
/// already-past resolve yields a finite APR rather than dividing by zero.
fn years_to(resolves: &str, today: &str) -> Option<f64> {
    let days = parse_iso(resolves)? - parse_iso(today)?;
    Some(days.max(1) as f64 / 365.25)
}

/// The first `20YY-MM-DD` in `hay`, as an epoch day. Hand-rolled because
/// arb-core carries no regex dependency and this is one fixed shape.
fn first_iso_date(hay: &str) -> Option<i64> {
    let b = hay.as_bytes();
    for i in 0..b.len() {
        let Some(w) = b.get(i..i + 10) else { break };
        let shaped = w[0] == b'2'
            && w[1] == b'0'
            && w[4] == b'-'
            && w[7] == b'-'
            && [2, 3, 5, 6, 8, 9].iter().all(|&j| w[j].is_ascii_digit());
        if !shaped {
            continue;
        }
        if let Some(d) = hay.get(i..i + 10).and_then(parse_iso) {
            return Some(d);
        }
    }
    None
}

/// `(resolves_by, estimated)` for a basket, in `mark_positions.py`'s order of
/// preference:
///
///   1. the curated table (`arb_core::resolve`), authoritative for known
///      families, with its own `estimated` flag;
///   2. whatever the trade record itself stamped;
///   3. sports baskets: entry + 1 day, because they resolve the same day the
///      game ends. The resulting APRs are huge by construction, and that IS the
///      point of fast-settling arb — capital recycles daily (Geoff 2026-07-22);
///   4. a `20YY-MM-DD` embedded in the relationship id or a leg's market id,
///      again plus one day. Probe-originated baskets (card e5696107) stamp no
///      `resolves_by`, but their event slugs carry the date.
///
/// A record with no date anywhere stays `None`: an invented entry+1d on a
/// long-dated market would fire false hard-unwind signals.
fn resolves_for(t: &Value, now: f64) -> (Option<String>, bool) {
    let rel = str_of(t, "relationship_id").unwrap_or_default();
    let (mut resolves, mut estimated) = match resolve_date(rel) {
        Some((d, est)) => (Some(d.to_string()), est),
        None => (
            str_of(t, "resolves_by").map(str::to_string),
            t.get("resolves_estimated").and_then(Value::as_bool).unwrap_or(false),
        ),
    };
    if resolves.is_none() && rel.starts_with("sports-") {
        let ts = f64_of(t, "ts").unwrap_or(now);
        resolves = Some(iso_from_day((ts / 86400.0).floor() as i64 + 1));
        estimated = true;
    }
    if resolves.is_none() {
        let mut hay = String::from(rel);
        for l in t.get("legs").and_then(|v| v.as_array()).into_iter().flatten() {
            hay.push(' ');
            hay.push_str(str_of(l, "market_id").unwrap_or_default());
        }
        if let Some(day) = first_iso_date(&hay) {
            resolves = Some(iso_from_day(day + 1));
            estimated = true;
        }
    }
    (resolves, estimated)
}

/// Pure per-position marking: one trade record plus four top-of-book quotes ->
/// one row.
///
/// Separated from [`build`] for the same reason `mark_positions.py` separated
/// its own `compute_row`: that file shipped two live bugs — inverted-basket
/// marking and a phantom reverse signal — that unit tests would have caught.
// Every argument is an input to the pricing itself; grouping them into a struct
// would name the same four quotes twice.
#[allow(clippy::too_many_arguments)]
pub fn compute_row(
    cx: &mut Cx,
    fees: &FeeSchedule,
    t: &Value,
    k_bid: Option<D>,
    k_ask: Option<D>,
    p_bid: Option<D>,
    p_ask: Option<D>,
    now: f64,
) -> Row {
    let rel = str_of(t, "relationship_id").unwrap_or_default().to_string();
    let legs = t.get("legs").and_then(|v| v.as_array()).map_or(&[][..], Vec::as_slice);
    let kleg = legs.iter().find(|l| str_of(l, "venue") == Some("kalshi"));
    let zero = cx.zero();
    let qty = dec_of(cx, t, "qty").unwrap_or(zero);
    let cost = dec_of(cx, t, "cost_usd").unwrap_or(zero);
    let locked = dec_of(cx, t, "profit_usd").unwrap_or(zero);
    let (resolves, estimated) = resolves_for(t, now);

    let mut row = Row {
        relationship_id: rel.clone(),
        ts: f64_of(t, "ts"),
        title: str_of(t, "title").map(str::to_string),
        kalshi_ticker: kleg.and_then(|l| str_of(l, "market_id")).map(str::to_string),
        qty: f(qty) as i64,
        cost_usd: f(cost),
        locked_profit_usd: f(locked),
        resolves_by: resolves.clone(),
        resolves_estimated: estimated,
        liq_value_usd: None,
        mark_pnl_usd: None,
        converged_pct: None,
        forward_hold_apr: None,
        natural_hold_apr: None,
        unwind_apr: None,
        unwind_signal: false,
        unwind_hard: false,
        reverse_edge_c: None,
        reverse_signal: false,
        maker_exit_ct: None,
        maker_exit_eligible: None,
    };

    // Basket direction. Standard = long Kalshi YES + long PM NO; a Kalshi-maker
    // ask-side fill produces the INVERSE (Kalshi NO + PM YES), and marking one
    // of those as standard flags our own entry as a phantom "reverse" arb.
    let inverted = kleg.and_then(|l| str_of(l, "side")) == Some("no");
    let priced = match (inverted, k_bid, k_ask, p_bid, p_ask) {
        // Liquidate: sell the Kalshi NO at the NO bid (= 1 - YES ask); sell the
        // PM YES at its bid.
        (true, _, Some(ka), Some(pb), _) => {
            let no_bid = cx.one_minus(ka);
            Some((cx.add(no_bid, pb), ka, pb))
        }
        // Liquidate: sell the Kalshi YES at its bid; close the PM NO by buying
        // YES at the ask, so the NO is worth 1 - ask.
        (false, Some(kb), _, _, Some(pa)) => {
            let no_val = cx.one_minus(pa);
            Some((cx.add(kb, no_val), kb, pa))
        }
        _ => None,
    };
    let Some((liq_ct, k_exit, p_exit)) = priced else { return row };

    let gross = cx.mul(liq_ct, qty);
    let fee = exit_fees_usd(cx, fees, k_exit, p_exit, qty);
    let liq = cx.sub(gross, fee);
    let mark = cx.sub(liq, cost);
    let (liq_f, mark_f, cost_f, locked_f) = (f(liq), f(mark), f(cost), f(locked));

    let yrs = resolves.as_deref().and_then(|r| years_to(r, &today_iso(now)));
    // Forward APR of CONTINUING to hold: remaining profit annualized over the
    // capital currently tied up — the liq value we would otherwise free.
    let fwd_apr = match (yrs, cx.cmp(liq, zero)) {
        (Some(y), Ordering::Greater) => Some((locked_f - mark_f) / liq_f / y * 100.0),
        _ => None,
    };
    let ts0 = f64_of(t, "ts").unwrap_or(now);
    let cost_pos = cx.cmp(cost, zero) == Ordering::Greater;
    // Realized APR to date = the rate you would LOCK IN by unwinding now: mark
    // gain on cost, annualized over the holding period so far. Noisy for a fresh
    // position — pair it with the hold APR.
    let held_yr = (now - ts0).max(MIN_HELD_S) / YEAR_S;
    let unwind_apr = cost_pos.then(|| mark_f / cost_f / held_yr * 100.0);
    // NATURAL hold APR: the intrinsic riskless yield if held to resolution —
    // locked profit / cost / (entry -> resolution), ignoring exit taker fees and
    // slippage entirely, because you do not pay them if you hold. This is the
    // "true" arb yield, against `forward_hold_apr`, which is depressed by the
    // liquidation mark.
    let total_yr = yrs.map(|y| (now - ts0).max(0.0) / YEAR_S + y);
    let natural_apr = match (cost_pos, total_yr) {
        (true, Some(ty)) if ty != 0.0 => Some(locked_f / cost_f / ty * 100.0),
        _ => None,
    };

    // Sports baskets resolve in HOURS and their books move and freeze fast: hold
    // to settlement, the sweeper realizes them. The APR-based unwind and reverse
    // machinery is for the slow political book — a snapshot signal here nearly
    // churned a converged basket for +2c against frozen-book and fill-lag risk
    // (2026-07-22 Preston).
    let is_sports = rel.starts_with("sports-");
    let mark_ge_locked = cx.cmp(mark, locked) != Ordering::Less;
    let mark_pos = cx.cmp(mark, zero) == Ordering::Greater;
    // Unwind when we have banked the full locked profit, OR we are in profit and
    // the remaining hold has become a low-APR use of capital (early convergence
    // on a long-dated position — the time-value case).
    let unwind = !is_sports
        && (mark_ge_locked || (mark_pos && fwd_apr.is_some_and(|a| a < HURDLE_APR)));
    // Hard unwind: forward APR below the floor — worse than trivially
    // redeployable yield, so exit regardless of utilization. `fwd_apr` is
    // naturally high when the mark is deeply negative (the remaining gain is
    // large), so this only fires on near-converged positions.
    let unwind_hard = fwd_apr.is_some_and(|a| a < HARD_FLOOR_APR) && !is_sports;

    let hundred = cx.from_i64(100);
    row.liq_value_usd = Some(liq_f);
    row.mark_pnl_usd = Some(mark_f);
    row.converged_pct = Some(if cx.cmp(locked, zero) == Ordering::Equal {
        0.0
    } else {
        let r = cx.div(mark, locked);
        f(cx.mul(r, hundred))
    });
    row.forward_hold_apr = fwd_apr.map(|a| round_to(a, 1));
    row.natural_hold_apr = natural_apr.map(|a| round_to(a, 1));
    row.unwind_apr = unwind_apr.map(|a| round_to(a, 1));
    row.unwind_signal = unwind;
    row.unwind_hard = unwind_hard;

    // The REVERSE basket is the OPPOSITE direction of what we hold:
    //   standard (K YES + PM NO) -> buy PM YES at ask + Kalshi NO at 1 - K bid
    //   inverted (K NO + PM YES) -> buy Kalshi YES at ask + PM NO at 1 - P bid
    let rev = match (inverted, k_bid, k_ask, p_bid, p_ask) {
        (true, _, Some(ka), Some(pb), _) => Some(cx.sub(pb, ka)),
        (false, Some(kb), _, _, Some(pa)) => Some(cx.sub(kb, pa)),
        _ => None,
    };
    row.reverse_edge_c = rev.map(|r| f(cx.mul(r, hundred)));
    // Sports reverse signals stay dark too: the WS engine owns sports crossings
    // at sub-second resolution, so a snapshot of a fast book is noise rather
    // than an actionable re-entry.
    let rev_floor = cx.parse_exact(REVERSE_EDGE);
    row.reverse_signal =
        !is_sports && rev.is_some_and(|r| cx.cmp(r, rev_floor) != Ordering::Less);

    // Maker-exit delta (Geoff 2026-07-24, Exits view): the per-contract profit
    // of the OPPORTUNISTIC exit right now — rest a Kalshi ask one tick inside
    // the competing ask, close the PM NO one tick through its ask on fill, which
    // is the runner's maker-unwind pricing. Positive = a passive exit locks
    // money today; eligible = the runner would actually rest it.
    //
    // From here on the two fields are PRESENT (as null/false) whatever happens,
    // because the Python sets them on every priced row.
    row.maker_exit_ct = Some(None);
    row.maker_exit_eligible = Some(false);
    let (Some(ka), Some(pa)) = (k_ask, p_ask) else { return row };
    if inverted || cx.cmp(qty, zero) == Ordering::Equal {
        return row;
    }
    let tick = cx.parse_exact(TICK);
    let k_exit_px = cx.sub(ka, tick);
    let p_close_px = cx.add(pa, tick);
    // Divergence 2: an out-of-band price is refused, not fed to a fee curve that
    // has no domain check.
    let one = cx.one;
    if cx.cmp(k_exit_px, zero) == Ordering::Less || cx.cmp(p_close_px, one) == Ordering::Greater {
        return row;
    }
    // Net of BOTH legs' fees (card b83b0449): the Kalshi ask rests as a MAKER
    // (0.0175 coef — Kalshi charges makers, unlike PM) and the PM NO close pays
    // TAKER. That is 1.2-1.9c/ct, so the fee-free version of this number called
    // exits "profitable" at 0.5c/ct that were net losers, and a placer would
    // rest real orders off this flag.
    let mx_fee_total = maker_exit_fees_usd(cx, fees, k_exit_px, p_close_px, qty);
    let mx_fees = cx.div(mx_fee_total, qty);
    let basis = cx.div(cost, qty);
    let p_no = cx.one_minus(p_close_px);
    let mut mx = cx.add(k_exit_px, p_no);
    mx = cx.sub(mx, basis);
    mx = cx.sub(mx, mx_fees);
    row.maker_exit_ct = Some(Some(round_to(f(mx), 4)));
    // Eligibility keys on the MAKER exit's own economics (Geoff 2026-07-24: the
    // taker mark held positions whose passive exit already locked profit): the
    // exit locks >= 0.5c/ct against basis AND holding is no longer the better
    // trade — unwind-flagged, or the forward APR is under the hurdle. Strong
    // holds never churn; sports stay dark.
    let floor = cx.parse_exact(MIN_MAKER_EXIT_CT);
    row.maker_exit_eligible = Some(
        cx.cmp(mx, floor) != Ordering::Less
            && !is_sports
            && (unwind || unwind_hard || fwd_apr.is_some_and(|a| a < HURDLE_APR)),
    );
    row
}

/// Open ledger records with `qty` reduced by matching `unwound` records.
///
/// Port of `arbbot/exec/ledger.py::open_baskets`. This is the same netting
/// [`crate::ledger::open_exposure`] does — but that one returns CONTRACTS per
/// relationship, and marking needs the RECORDS, because every field a row
/// carries (cost, profit, legs, title, entry ts) lives on the record. A partial
/// unwind returns a copy with reduced `qty` and proportionally reduced
/// cost / payoff / profit.
pub fn open_baskets(records: Vec<Value>) -> Vec<Value> {
    let records = crate::ledger::apply_corrections(records);
    // Matched on `(relationship_id, ts)` where ts is a float, keyed by bits for
    // the same reason `crate::ledger` does: Python uses the float itself as a
    // dict key, so bit-equality is the same predicate.
    let mut unwound: std::collections::HashMap<(String, u64), f64> = std::collections::HashMap::new();
    for r in &records {
        if str_of(r, "status") != Some("unwound") {
            continue;
        }
        if let (Some(rel), Some(ct)) = (str_of(r, "relationship_id"), f64_of(r, "closes_ts")) {
            *unwound.entry((rel.to_string(), ct.to_bits())).or_default() +=
                f64_of(r, "qty").unwrap_or(0.0);
        }
    }
    let mut out = Vec::new();
    for r in records {
        if str_of(&r, "status") != Some("open") {
            continue;
        }
        let key = (
            str_of(&r, "relationship_id").unwrap_or_default().to_string(),
            f64_of(&r, "ts").unwrap_or(0.0).to_bits(),
        );
        let closed = unwound.get(&key).copied().unwrap_or(0.0);
        let qty = f64_of(&r, "qty").unwrap_or(0.0);
        if closed <= 0.0 {
            out.push(r);
            continue;
        }
        let remaining = qty - closed;
        if remaining <= 0.0 {
            continue;
        }
        let frac = remaining / qty;
        let mut scaled = r;
        if let Some(obj) = scaled.as_object_mut() {
            obj.insert(
                "qty".into(),
                if remaining.fract() == 0.0 {
                    serde_json::json!(remaining as i64)
                } else {
                    serde_json::json!(remaining)
                },
            );
            for k in ["cost_usd", "payoff_usd", "profit_usd"] {
                let Some(v) = obj.get(k).and_then(Value::as_f64) else { continue };
                obj.insert(k.into(), serde_json::json!(v * frac));
            }
        }
        out.push(scaled);
    }
    out
}

/// One marking pass: the file, and what the engine could not see to write it.
pub struct Marked {
    pub doc: Doc,
    /// `venue:market_id` for every leg of a MARKABLE basket the engine holds no
    /// book for. Those rows are emitted with null prices — the honest answer —
    /// but a null row and a genuinely dark venue are not the same event, and
    /// only this tells them apart.
    ///
    /// **THIS IS NOT HYPOTHETICAL, AND IT IS THE ONE THING THAT BLOCKS RETIRING
    /// `arbbot-marks.timer`.** `mark_positions.py` fetches each position's quote
    /// by REST and can therefore price anything the venue will answer about;
    /// this engine can only price what the RECORDER carries. The recorder's
    /// PM-US universe is TAG-driven (`config/recorder.yaml`'s
    /// `polymarket_us_tags` -> `pmus.markets_by_tags`), NOT registry-driven, so a
    /// registry market whose PM-US event has fallen out of those tags is
    /// invisible here while remaining perfectly priceable over REST.
    ///
    /// Measured 2026-07-31: `ewc-pres-fra-2027-04-11-bruret` and
    /// `...-frahol` appear ZERO times in 300 MB of that day's PM-US tape, while
    /// both Kalshi legs tick normally. That is 8 of 25 rows — every
    /// `xvus-france-pres-27` basket, which is the family `crate::unwind`'s own
    /// header names as its live candidate set.
    pub no_book: std::collections::BTreeSet<String>,
}

/// Mark every open basket against the live books.
pub fn build(
    cx: &mut Cx,
    fees: &FeeSchedule,
    records: Vec<Value>,
    books: &BookBuilder,
    now: f64,
) -> Marked {
    let open = open_baskets(records);
    let n_open = open.len() as u64;
    let (mut positions, mut tot_cost, mut tot_mark, mut tot_locked) = (Vec::new(), 0.0, 0.0, 0.0);
    let mut unpriced = 0u64;
    let mut no_book = std::collections::BTreeSet::new();
    for t in &open {
        let legs = t.get("legs").and_then(|v| v.as_array()).map_or(&[][..], Vec::as_slice);
        if legs.len() < 2 {
            // Single-leg records (pm-lean riders) are directional — there is no
            // basket to mark. Skipped BEFORE the unpriced counter, as in Python.
            continue;
        }
        // Marking needs a cost basis; without one the row would be a guess. Skip
        // and COUNT — see `Totals::unpriced_positions` for what a silent
        // omission here costs.
        let (Some(cost), Some(locked)) = (f64_of(t, "cost_usd"), f64_of(t, "profit_usd")) else {
            unpriced += 1;
            continue;
        };
        let kleg = legs.iter().find(|l| str_of(l, "venue") == Some("kalshi"));
        let pleg = legs.iter().find(|l| str_of(l, "venue") == Some("polymarket_us"));
        // Divergence 4: Python's bare `next(...)` raises here and kills the run.
        let (Some(kleg), Some(pleg)) = (kleg, pleg) else {
            unpriced += 1;
            continue;
        };
        let km = str_of(kleg, "market_id").unwrap_or("");
        let pm = str_of(pleg, "market_id").unwrap_or("");
        if books.get(Venue::Kalshi, km).is_none() {
            no_book.insert(format!("kalshi:{km}"));
        }
        if books.get(Venue::PolymarketUs, pm).is_none() {
            no_book.insert(format!("polymarket_us:{pm}"));
        }
        let (k_bid, k_ask) = top(cx, books, Venue::Kalshi, km);
        let (p_bid, p_ask) = top(cx, books, Venue::PolymarketUs, pm);
        let row = compute_row(cx, fees, t, k_bid, k_ask, p_bid, p_ask, now);
        if let Some(m) = row.mark_pnl_usd {
            tot_mark += m;
        }
        tot_cost += cost;
        tot_locked += locked;
        positions.push(row);
    }
    let totals = Totals {
        cost_usd: round_to(tot_cost, 2),
        mark_pnl_usd: round_to(tot_mark, 2),
        locked_profit_usd: round_to(tot_locked, 2),
        n_open,
        unpriced_positions: unpriced,
        unwind_signals: positions.iter().filter(|p| p.unwind_signal).count() as u64,
        unwind_hard: positions.iter().filter(|p| p.unwind_hard).count() as u64,
        reverse_signals: positions.iter().filter(|p| p.reverse_signal).count() as u64,
    };
    Marked { doc: Doc { generated_at: stamp(now), positions, totals }, no_book }
}

/// Every market an open basket has a leg on — what makes a book event worth
/// re-marking for.
///
/// Keyed BY VENUE rather than as a set of pairs so the engine can ask
/// `contains(market_id)` on a borrowed `&str`. This is asked once per book
/// event, ~250 times a second, and a `HashSet<(Venue, String)>` would have to
/// allocate a `String` for every one of them just to look it up.
///
/// Built from the SAME `open_baskets` fold [`build`] runs over, so the two
/// cannot disagree about which markets matter. Single-leg and unpriced records
/// are included: they produce no row, but re-marking on them is harmless, and
/// excluding them would make this set depend on pricing state that moves.
pub fn watched_markets(
    records: Vec<Value>,
) -> std::collections::HashMap<Venue, std::collections::HashSet<String>> {
    let mut out: std::collections::HashMap<Venue, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for t in open_baskets(records) {
        for l in t.get("legs").and_then(|v| v.as_array()).into_iter().flatten() {
            let (Some(v), Some(m)) = (str_of(l, "venue"), str_of(l, "market_id")) else { continue };
            if let Some(venue) = Venue::parse(v) {
                out.entry(venue).or_default().insert(m.to_string());
            }
        }
    }
    out
}

/// Write the file so no reader can ever see half of one.
///
/// Temp-plus-rename, not truncate-and-write. Every consumer polls this file on
/// its own schedule — the dashboard on a request, `unwind::select` and
/// `bar_from_marks` on the engine's own stats tick — and `bar_from_marks`
/// answers unparseable text with `Bar::Untrusted`, which REFUSES take-take. A
/// torn read is therefore not cosmetic: it stops trading. `rename(2)` within a
/// directory is atomic, so a reader sees the whole old file or the whole new
/// one.
///
/// No `fsync`. This is a derived snapshot, rebuilt from the ledger and the books
/// within a second of any restart — unlike `ledger::append`, which is the record
/// of money already spent.
pub fn write_atomic(path: &str, body: &str) -> Result<(), String> {
    let p = std::path::Path::new(path);
    if let Some(dir) = p.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
        }
    }
    // Same directory, so the rename cannot cross a filesystem; pid-tagged, so
    // two processes writing one path cannot share a temp file.
    let tmp = p.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, body).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, p).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("rename {} -> {path}: {e}", tmp.display())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::model::Level;

    fn v(s: &str) -> Value {
        serde_json::from_str(s).expect("test json")
    }

    fn q(cx: &mut Cx, s: &str) -> Option<D> {
        cx.parse(s)
    }

    fn lv(price: &str, size: &str) -> Level {
        Level { price: price.into(), size: size.into() }
    }

    /// Keys of a JSON object IN THE ORDER THEY WERE WRITTEN, at one nesting
    /// depth.
    ///
    /// Reading them back through `serde_json::Value` cannot answer this and
    /// silently pretends to: its `Map` is a `BTreeMap` unless the
    /// `preserve_order` feature is on, so both sides of the comparison come out
    /// alphabetical and a reordered field passes. The first draft of the schema
    /// test did exactly that.
    fn keys_in_order(json: &str, depth_wanted: usize) -> Vec<String> {
        let (mut out, mut depth, mut in_str, mut esc, mut expect_key, mut start) =
            (Vec::new(), 0usize, false, false, false, 0usize);
        for (i, c) in json.char_indices() {
            if in_str {
                match c {
                    _ if esc => esc = false,
                    '\\' => esc = true,
                    '"' => {
                        in_str = false;
                        if expect_key && depth == depth_wanted {
                            out.push(json[start + 1..i].to_string());
                            expect_key = false;
                        }
                    }
                    _ => {}
                }
                continue;
            }
            match c {
                '"' => {
                    in_str = true;
                    start = i;
                }
                '{' => {
                    depth += 1;
                    expect_key = true;
                }
                '}' => depth -= 1,
                '[' => depth += 1,
                ']' => depth -= 1,
                ',' => expect_key = true,
                ':' => expect_key = false,
                _ => {}
            }
        }
        out
    }

    /// Books holding one Kalshi market and one PM-US market, each with whatever
    /// levels the caller gives — `&[]` for a side that is EMPTY, which is
    /// divergence 1's whole subject.
    fn books(k: (&[Level], &[Level]), p: (&[Level], &[Level])) -> BookBuilder {
        let mut b = BookBuilder::new();
        b.apply_snapshot(Venue::Kalshi, "K", k.0.to_vec(), k.1.to_vec(), 1, 0, None);
        b.apply_snapshot(Venue::PolymarketUs, "P", p.0.to_vec(), p.1.to_vec(), 1, 0, None);
        b
    }

    /// Epoch seconds for a `YYYY-MM-DD` plus a time of day, so a test can pin
    /// the clock the APR columns are measured from.
    fn at(day: &str, h: i64, m: i64, s: i64) -> f64 {
        (parse_iso(day).expect("day") * 86400 + h * 3600 + m * 60 + s) as f64
    }

    /// THE 2026-07-31 KULEBA BASKET, byte-for-byte off `data/exec/trades.jsonl`.
    fn kuleba() -> Value {
        v(r#"{"ts":1785402005.539014,"relationship_id":"xvus-nobel-peace-26-mykolakuleba",
            "title":"xvus-nobel-peace-26-mykolakuleba (naked-leg hedge)","qty":1,
            "strategy":"take-take","status":"open","cost_usd":0.72,"profit_usd":0.28,
            "legs":[{"venue":"kalshi","market_id":"K","side":"yes",
                     "role":"taker","qty":1,"yes_price":"0.0800"},
                    {"venue":"polymarket_us","market_id":"P",
                     "side":"no","role":"taker","qty":1,"yes_price":"0.36"}]}"#)
    }

    /// **THE PARITY TEST.** One real basket, at the quotes the live book carried
    /// on 2026-07-31T18:24:49Z, against the row `mark_positions.py` published
    /// for it at that instant — every field, to the last digit it printed.
    ///
    /// The quotes are `k_bid 0.05 / k_ask 0.08 / p_ask 0.09`, recovered from the
    /// published row itself: `reverse_edge_c: -4.0` fixes `k_bid - p_ask`,
    /// `liq_value_usd: 0.945086` fixes the pair through the fee curves, and
    /// `maker_exit_ct: 0.2346` fixes `k_ask`. They are not invented inputs —
    /// they are the only quotes that produce this row.
    ///
    /// This is the test that would catch the port drifting from the writer it
    /// replaces. Every one of these numbers passes through a different piece of
    /// the arithmetic: `liq_value_usd` through both taker fee curves (one of
    /// which ceils to the cent), `converged_pct` through a Decimal division
    /// whose 28-digit tail is printed in full, `forward_hold_apr` through the
    /// resolve-date table and the day count, `maker_exit_ct` through the maker
    /// curve and the two tick steps.
    #[test]
    fn the_live_row_reproduces_the_python_writers_numbers() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let now = at("2026-07-31", 18, 24, 49);
        let (kb, ka, pb, pa) =
            (q(&mut cx, "0.05"), q(&mut cx, "0.08"), q(&mut cx, "0.08"), q(&mut cx, "0.09"));
        let row = compute_row(&mut cx, &fees, &kuleba(), kb, ka, pb, pa, now);

        assert_eq!(row.relationship_id, "xvus-nobel-peace-26-mykolakuleba");
        assert_eq!(row.ts, Some(1_785_402_005.539_014));
        assert_eq!(row.qty, 1);
        assert_eq!(row.cost_usd, 0.72);
        assert_eq!(row.locked_profit_usd, 0.28);
        assert_eq!(row.resolves_by.as_deref(), Some("2026-10-09"), "the curated table wins");
        assert!(row.resolves_estimated, "and it flags itself a guess");
        // Both taker curves, exactly: Kalshi ceils 0.07*0.05*0.95 to $0.01, PM-US
        // charges 0.06*0.09*0.91 = 0.004914, off a 0.96/ct liquidation.
        assert_eq!(row.liq_value_usd, Some(0.945_086));
        assert_eq!(row.mark_pnl_usd, Some(0.225_086));
        assert_eq!(row.converged_pct, Some(80.387_857_142_857_14));
        assert_eq!(row.forward_hold_apr, Some(30.3), "70 days to 2026-10-09, UTC");
        assert_eq!(row.natural_hold_apr, Some(199.0));
        assert_eq!(row.reverse_edge_c, Some(-4.0));
        assert_eq!(row.maker_exit_ct, Some(Some(0.2346)));
        assert!(!row.unwind_signal);
        assert!(!row.unwind_hard);
        assert!(!row.reverse_signal);
        assert_eq!(row.maker_exit_eligible, Some(false), "the hold still beats the exit");
    }

    /// **THE SCHEMA.** The key list, in order, of a row and of `totals` — this
    /// file's whole contract with three other processes. Compared against the
    /// keys of a row taken verbatim out of the live `data/exec/marks.json`, so
    /// a renamed, dropped, added or reordered field fails here rather than in
    /// the dashboard.
    #[test]
    fn the_schema_matches_the_python_writer_field_for_field() {
        // Off data/exec/marks.json, 2026-07-31T18:09:16Z.
        let live = r#"{
        "relationship_id": "xvus-time-poty-26-zohranmamdani","ts": 1784646759.465,
        "title": "Mamdani is TIME Person of the Year 2026","kalshi_ticker": "KXTIME-26-ZOH",
        "qty": 47,"cost_usd": 45.7064,"locked_profit_usd": 1.2936,
        "resolves_by": "2026-12-09","resolves_estimated": true,
        "liq_value_usd": 41.996498,"mark_pnl_usd": -3.709902,
        "converged_pct": -286.788961038961,"forward_hold_apr": 33.2,
        "natural_hold_apr": 7.3,"unwind_apr": -293.0,"unwind_signal": false,
        "unwind_hard": false,"reverse_edge_c": -8.0,"reverse_signal": false,
        "maker_exit_ct": -0.0793,"maker_exit_eligible": false}"#;
        let want = keys_in_order(live, 1);
        assert_eq!(want.len(), 21, "the live row has 21 fields");
        assert!(v(live).is_object(), "and the literal above is real JSON");

        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let now = at("2026-07-31", 18, 24, 49);
        let (kb, ka, pb, pa) =
            (q(&mut cx, "0.05"), q(&mut cx, "0.08"), q(&mut cx, "0.08"), q(&mut cx, "0.09"));
        let row = compute_row(&mut cx, &fees, &kuleba(), kb, ka, pb, pa, now);
        let got = keys_in_order(&serde_json::to_string(&row).expect("ser"), 1);
        assert_eq!(got, want, "a priced row's keys, in order");

        // ...and the unpriced row, where the Python sets NEITHER maker_exit
        // field, so they must be ABSENT rather than null.
        let dark = compute_row(&mut cx, &fees, &kuleba(), None, None, None, None, now);
        let dark_keys = keys_in_order(&serde_json::to_string(&dark).expect("ser"), 1);
        assert_eq!(
            dark_keys,
            want[..want.len() - 2].to_vec(),
            "an unpriced row omits maker_exit_ct and maker_exit_eligible entirely"
        );

        let marked = build(&mut cx, &fees, vec![kuleba()], &BookBuilder::new(), now);
        let body = marked.doc.to_json();
        assert_eq!(
            keys_in_order(&serde_json::to_string(&marked.doc.totals).expect("ser"), 1),
            vec![
                "cost_usd",
                "mark_pnl_usd",
                "locked_profit_usd",
                "n_open",
                "unpriced_positions",
                "unwind_signals",
                "unwind_hard",
                "reverse_signals"
            ],
            "the totals block every operator monitor reads"
        );
        assert_eq!(keys_in_order(&body, 1), vec!["generated_at", "positions", "totals"]);
        assert_eq!(
            marked.doc.generated_at, "2026-07-31T18:24:49Z",
            "the one shape bar_from_marks accepts"
        );
    }

    /// **THE GUARD THAT KILLED THE TIMER.** A basket with no cost basis is not
    /// marked with a guessed one — it is skipped, and it is COUNTED. On
    /// 2026-07-28 the four records this engine had just written hit
    /// `mark_positions.py`'s hard subscript instead, and the resulting frozen
    /// take-take bar was traded against for four hours.
    #[test]
    fn a_basket_with_no_cost_basis_is_skipped_and_counted() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        // What this engine actually writes: order-side spelling, no cost_usd.
        let engine_written = v(r#"{"ts":2.0,"relationship_id":"xvus-nobel-peace-26-mykolakuleba",
            "qty":1,"status":"open","source":"arb-trader",
            "legs":[{"venue":"polymarket_us","market_id":"P","side":"ask"},
                    {"venue":"kalshi","market_id":"K","side":"bid"}]}"#);
        let bk = books((&[lv("0.05", "9")], &[lv("0.08", "9")]), (&[], &[lv("0.09", "9")]));
        let m = build(
            &mut cx,
            &fees,
            vec![kuleba(), engine_written],
            &bk,
            at("2026-07-31", 18, 24, 49),
        );
        assert_eq!(m.doc.positions.len(), 1, "only the priced basket is marked");
        assert_eq!(m.doc.totals.unpriced_positions, 1, "and the other is COUNTED, not dropped");
        assert_eq!(m.doc.totals.n_open, 2, "n_open is every open record");
        assert_eq!(m.doc.positions[0].liq_value_usd, Some(0.945_086), "unchanged by its neighbour");
    }

    /// `n_open` is not `positions.len()`, and the difference is exactly the
    /// records no row was emitted for. The live file reads 32 against 25; this
    /// pins the identity that makes those two numbers legible.
    #[test]
    fn n_open_counts_every_open_record_and_the_gap_is_accounted_for() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let single = v(r#"{"ts":3.0,"relationship_id":"pm-lean-rider","qty":5,"status":"open",
            "cost_usd":1.0,"profit_usd":0.1,
            "legs":[{"venue":"polymarket_us","market_id":"P","side":"no"}]}"#);
        let no_basis = v(r#"{"ts":4.0,"relationship_id":"xvus-x","qty":1,"status":"open",
            "legs":[{"venue":"kalshi","market_id":"K","side":"yes"},
                    {"venue":"polymarket_us","market_id":"P","side":"no"}]}"#);
        // A two-leg basket with no PM-US leg — divergence 4, where the Python
        // raises StopIteration and kills the whole run.
        let intl = v(r#"{"ts":5.0,"relationship_id":"xv-intl","qty":1,"status":"open",
            "cost_usd":1.0,"profit_usd":0.1,
            "legs":[{"venue":"kalshi","market_id":"K","side":"yes"},
                    {"venue":"polymarket","market_id":"Q","side":"no"}]}"#);
        let bk = books((&[lv("0.05", "9")], &[lv("0.08", "9")]), (&[], &[lv("0.09", "9")]));
        let t = build(&mut cx, &fees, vec![kuleba(), single, no_basis, intl], &bk, 1.0).doc.totals;
        assert_eq!(t.n_open, 4);
        assert_eq!(t.unpriced_positions, 2, "no cost basis, and no PM-US leg");
        // n_open - rows - unpriced == the single-leg records, in both writers.
        assert_eq!(t.n_open as usize - 1 - t.unpriced_positions as usize, 1);
    }

    /// **DIVERGENCE 1.** An empty Kalshi side is `None`, not `Decimal("0")`.
    ///
    /// The Python's `or 0` turns a missing quote into a zero PRICE, which then
    /// reaches `leg_fee` as `-0.01` and raises out of an unguarded loop —
    /// killing the run before the file is written, and leaving every consumer
    /// on the last good copy. Here it is one unpriced row, and every other row
    /// still gets written.
    #[test]
    fn an_empty_book_side_publishes_an_unpriced_row_rather_than_a_zero_price() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let now = at("2026-07-31", 18, 24, 49);
        // Kalshi with no BID at all: a standard basket liquidates on that bid.
        let bk = books((&[], &[lv("0.08", "9")]), (&[lv("0.08", "9")], &[lv("0.09", "9")]));
        let m = build(&mut cx, &fees, vec![kuleba()], &bk, now);
        let row = &m.doc.positions[0];
        assert_eq!(row.liq_value_usd, None, "no bid is no liquidation value, not a zero one");
        assert_eq!(row.mark_pnl_usd, None);
        assert_eq!(row.forward_hold_apr, None);
        assert!(!row.unwind_signal, "and nothing can be signalled off it");
        assert!(!row.unwind_hard);
        assert_eq!(row.maker_exit_ct, None, "the maker-exit fields are absent, as in Python");
        assert!(m.no_book.is_empty(), "the BOOK is there — only one of its sides is empty");
    }

    /// A market the recorder does not carry is NAMED, not merely nulled.
    ///
    /// This is the live gap and the thing that blocks retiring the timer: on
    /// 2026-07-31 the PM-US legs of every `xvus-france-pres-27` basket were
    /// absent from the recorder's tag-driven universe, so 8 of 25 rows come out
    /// unpriced here while `mark_positions.py` prices all 25 over REST.
    #[test]
    fn a_market_the_recorder_does_not_carry_is_named_not_merely_nulled() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        // Kalshi is on the feed; the PM-US leg is not tracked at all.
        let mut bk = BookBuilder::new();
        bk.apply_snapshot(
            Venue::Kalshi,
            "K",
            vec![lv("0.05", "9")],
            vec![lv("0.08", "9")],
            1,
            0,
            None,
        );
        let m = build(&mut cx, &fees, vec![kuleba()], &bk, at("2026-07-31", 18, 24, 49));
        assert_eq!(m.doc.positions.len(), 1, "the row is still published");
        assert_eq!(m.doc.positions[0].liq_value_usd, None, "...unpriced");
        assert_eq!(
            m.no_book.iter().cloned().collect::<Vec<_>>(),
            vec!["polymarket_us:P".to_string()],
            "...and the reason is the market, by name"
        );
    }

    /// An INVERTED basket (Kalshi NO + PM YES) liquidates the other way round,
    /// and the reverse edge flips with it. Marking one as standard is a live bug
    /// `mark_positions.py` shipped once: it flags our own entry as a phantom
    /// "reverse" arb.
    #[test]
    fn an_inverted_basket_liquidates_the_other_way_round() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let now = at("2026-07-31", 18, 24, 49);
        let mut inv = kuleba();
        inv["legs"][0]["side"] = v("\"no\"");
        let (kb, ka, pb, pa) =
            (q(&mut cx, "0.05"), q(&mut cx, "0.08"), q(&mut cx, "0.40"), q(&mut cx, "0.42"));
        let row = compute_row(&mut cx, &fees, &inv, kb, ka, pb, pa, now);
        // Sell the Kalshi NO at 1 - 0.08 = 0.92, sell the PM YES at 0.40.
        // 1.32/ct gross, less ceil_cents(0.07*0.08*0.92) = 0.01 Kalshi taker and
        // 0.06*0.40*0.60 = 0.0144 PM-US taker. Both legs' fees are priced off
        // the YES price of the leg being closed, whichever side we close.
        assert_eq!(row.liq_value_usd, Some(1.2956));
        assert_eq!(row.reverse_edge_c, Some(32.0), "p_bid - k_ask, not k_bid - p_ask");
        assert!(row.reverse_signal, "and it clears the 3c floor");
        assert_eq!(
            row.maker_exit_ct,
            Some(None),
            "the maker exit rests a Kalshi ask against a long YES; an inverted \
             basket has none to rest"
        );

        // The SAME quotes read as a standard basket give the other answer
        // entirely — which is why the flag is load-bearing.
        let std_row = compute_row(&mut cx, &fees, &kuleba(), kb, ka, pb, pa, now);
        assert_eq!(std_row.reverse_edge_c, Some(-37.0));
        assert!(!std_row.reverse_signal);
    }

    /// Sports baskets resolve in hours: they are held to settlement and the
    /// sweeper realizes them, so no snapshot signal fires for one. Both flags,
    /// because both were turned off for the same reason on the same day.
    #[test]
    fn a_sports_basket_never_signals_unwind_or_reverse() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let now = at("2026-07-31", 18, 24, 49);
        // Fully converged (mark >= locked) AND a 32c reverse edge: both signals
        // would fire on any other family.
        let sport = v(r#"{"ts":1785402005.539014,"relationship_id":"sports-nba-lal-bos",
            "qty":1,"status":"open","cost_usd":0.50,"profit_usd":0.02,
            "legs":[{"venue":"kalshi","market_id":"K","side":"yes"},
                    {"venue":"polymarket_us","market_id":"P","side":"no"}]}"#);
        let (kb, ka, pb, pa) =
            (q(&mut cx, "0.60"), q(&mut cx, "0.62"), q(&mut cx, "0.20"), q(&mut cx, "0.22"));
        let row = compute_row(&mut cx, &fees, &sport, kb, ka, pb, pa, now);
        assert!(row.mark_pnl_usd.expect("priced") >= 0.02, "it HAS converged");
        assert!(row.reverse_edge_c.expect("edge") >= 3.0, "and the reverse IS on");
        assert!(!row.unwind_signal, "sports stay dark");
        assert!(!row.unwind_hard);
        assert!(!row.reverse_signal);
        assert_eq!(row.maker_exit_eligible, Some(false));
        // ...and with no date anywhere it falls back to entry + 1 day.
        assert_eq!(row.resolves_by.as_deref(), Some("2026-07-31"));
        assert!(row.resolves_estimated);
    }

    /// A probe-originated basket stamps no `resolves_by`, but its event slug
    /// carries the date. Without this the row gets no APR columns at all.
    #[test]
    fn an_undated_basket_takes_its_date_from_a_leg_slug() {
        let t = v(r#"{"ts":1785402005.539014,"relationship_id":"probe-aec-thing",
            "qty":1,"status":"open","cost_usd":0.5,"profit_usd":0.1,
            "legs":[{"venue":"kalshi","market_id":"KXAEC","side":"yes"},
                    {"venue":"polymarket_us","market_id":"aec-house-2026-08-23-x","side":"no"}]}"#);
        assert_eq!(resolves_for(&t, 0.0), (Some("2026-08-24".into()), true), "event date + 1 day");

        // A record with no date ANYWHERE stays None: a wrong entry+1d guess on a
        // long-dated market would fire false hard-unwind signals.
        let mut bare = t.clone();
        bare["legs"][1]["market_id"] = v("\"no-date-here\"");
        assert_eq!(resolves_for(&bare, 0.0), (None, false));
    }

    /// **DIVERGENCE 2.** An out-of-band exit price refuses the branch instead of
    /// reaching a fee curve that has no domain check. `arb_core::fees` would
    /// happily return a negative fee for a negative price; Python raises. Both
    /// are better than publishing the number.
    #[test]
    fn the_maker_exit_branch_refuses_an_out_of_band_price() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let now = at("2026-07-31", 18, 24, 49);
        // A Kalshi ask AT zero: one tick inside it is -0.01.
        let (kb, ka, pb, pa) =
            (q(&mut cx, "0.05"), q(&mut cx, "0.00"), q(&mut cx, "0.08"), q(&mut cx, "0.09"));
        let row = compute_row(&mut cx, &fees, &kuleba(), kb, ka, pb, pa, now);
        assert!(row.liq_value_usd.is_some(), "the LIQUIDATION side is still priceable");
        assert_eq!(row.maker_exit_ct, Some(None), "but the maker exit is refused, by name");
        assert_eq!(row.maker_exit_eligible, Some(false));

        // ...and the same at the other end: a PM ask at 1.00 closes at 1.01.
        let pa1 = q(&mut cx, "1.00");
        let ka1 = q(&mut cx, "0.08");
        let row = compute_row(&mut cx, &fees, &kuleba(), kb, ka1, pb, pa1, now);
        assert_eq!(row.maker_exit_ct, Some(None));
    }

    /// A partial unwind reduces the basket rather than dropping it, and scales
    /// cost and profit with it. A mark against the FULL cost of a
    /// half-closed basket is wrong in the direction that hides a loss.
    #[test]
    fn a_partial_unwind_scales_the_basket() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50,
                  "cost_usd":50.0,"profit_usd":5.0,
                  "legs":[{"venue":"kalshi","market_id":"K","side":"yes"},
                          {"venue":"polymarket_us","market_id":"P","side":"no"}]}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":20}"#),
        ];
        let open = open_baskets(recs);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0]["qty"], v("30"), "an integer remainder stays an integer");
        assert_eq!(open[0]["cost_usd"], v("30.0"));
        assert_eq!(open[0]["profit_usd"], v("3.0"));

        // Fully unwound disappears entirely.
        let gone = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":50}"#),
        ];
        assert!(open_baskets(gone).is_empty());
    }

    /// `watched_markets` is the re-mark TRIGGER set, and it is built from the
    /// same fold `build` runs — including the records that produce no row, so
    /// the trigger cannot depend on pricing state that moves.
    #[test]
    fn the_trigger_set_covers_every_leg_of_every_open_basket() {
        let no_basis = v(r#"{"ts":4.0,"relationship_id":"xvus-x","qty":1,"status":"open",
            "legs":[{"venue":"kalshi","market_id":"K2","side":"yes"},
                    {"venue":"polymarket_us","market_id":"P2","side":"no"}]}"#);
        let w = watched_markets(vec![kuleba(), no_basis]);
        assert!(w[&Venue::Kalshi].contains("K"));
        assert!(w[&Venue::PolymarketUs].contains("P"));
        assert!(w[&Venue::Kalshi].contains("K2"), "an unpriced basket still moves the mark");
        assert!(w[&Venue::PolymarketUs].contains("P2"));
        assert!(!w[&Venue::Kalshi].contains("SOMETHING-ELSE"));
    }

    /// The bytes: `json.dumps(doc, indent=0)`, and a whole file or none of it.
    #[test]
    fn the_file_is_written_whole_in_the_python_writers_format() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let bk = books((&[lv("0.05", "9")], &[lv("0.08", "9")]), (&[], &[lv("0.09", "9")]));
        let doc = build(&mut cx, &fees, vec![kuleba()], &bk, at("2026-07-31", 18, 24, 49)).doc;
        let body = doc.to_json();
        assert!(body.starts_with("{\n\"generated_at\": \""), "indent=0, not 2: {:?}", &body[..40]);
        assert!(!body.contains("\n  "), "no indentation anywhere");

        let dir = std::env::temp_dir().join(format!("arb-marks-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let p = dir.join("nested").join("marks.json");
        let path = p.to_str().expect("utf8");
        write_atomic(path, &body).expect("write into a directory that does not exist yet");
        assert_eq!(std::fs::read_to_string(&p).expect("read"), body);
        // Rewriting leaves no temp behind for a reader to trip over.
        write_atomic(path, &body).expect("rewrite");
        let leftovers: Vec<_> = std::fs::read_dir(p.parent().expect("parent"))
            .expect("dir")
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .filter(|n| n != "marks.json")
            .collect();
        assert!(leftovers.is_empty(), "temp files must not survive a write: {leftovers:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
