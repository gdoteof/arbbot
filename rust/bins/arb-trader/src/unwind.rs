//! Opportunistic unwind: which open baskets have stopped being the best use of
//! the capital they lock.
//!
//! DETECT ONLY, AND NOT ARMABLE. This module is the spec the arming PR will be
//! written against, so the five things that stand between it and a placer are
//! stated here FIRST, before the rule they qualify. Four of them were claimed
//! the other way round in this file's first draft; each retraction is marked.
//!
//! # 1. What this selects and what this engine can act on are disjoint sets
//!
//! **RETRACTED IN BOTH HALVES, on two different dates.** This was THE BLOCKER
//! TO ARMING and it is no longer a blocker at all. The claim is kept, because
//! the placer in `crate::maker_exit` was written against it and a reader of
//! that file needs to know which sentence stopped being true when:
//!
//!   * "`mark_positions.py` skips any open basket with no `cost_usd`/
//!     `profit_usd` (`mark_positions.py:280-282`), and `engine::fill::
//!     book_basket` writes exactly that shape — `status: "open"`,
//!     `fees_pending: true`, no cost basis, because venue fees arrive on fill
//!     reports the engine does not read. So EVERY basket the Rust engine has
//!     ever booked is invisible to the file this module decides from." True of
//!     the Python, and of the Rust port that replaced it, until **2026-08-14**:
//!     [`crate::marks::build`] now DERIVES the missing basis from the record's
//!     own legs (`marks::derived_basis` — `naked_act::worst_lot`'s arithmetic,
//!     which is a measurement and not a guess), so every two-leg basket this
//!     engine has booked is an ordinary row and [`select`] reads it like any
//!     other. On the ledger the day it landed that was 31 records and about
//!     $133 of book. `totals.unpriced_positions` now counts only records whose
//!     LEGS cannot carry a basis.
//!   * "Those baskets sit in the `xvus-nobel-peace-26` and `xvus-brazil-pres-26`
//!     families — which are, precisely, two of the three `--rel-prefix` scopes
//!     the armed process runs under. Meanwhile every candidate this module
//!     selects from the live marks is `xvus-france-pres-27` or
//!     `xvus-time-poty-26`, for which that process holds no quoter
//!     (`main.rs:387` filters the registry by prefix) and no book
//!     subscription." Retracted **2026-07-31** in `maker_exit`'s own §1: the
//!     armed process runs `--rel-prefix xvus-` and every priced position in
//!     `data/exec/marks.json` is `xvus-`, so the candidate set and the owned set
//!     are the same set.
//!
//! A detector that reports what it could never execute is a misleading
//! detector, so [`Exit::actionable`] still carries the prefix test and the
//! report still says which side of it a candidate falls on — enforced rather
//! than assumed, which is what makes the retraction above safe to act on.
//! `scripts/unwind_positions.py:282` refuses by the same kind of ownership
//! contract and has since it was written.
//!
//! THE MISSING COST BASIS HAD A THIRD OPTION, and the route taken was a fourth.
//! This paragraph offered corrections — `ledger::apply_corrections`
//! (`ledger.py:26-52`) shallow-merges
//! `{"status":"correction","corrects_ts":<the open record's ts>,"fields":{…}}`
//! onto its target, and `open_baskets` folds them BEFORE the basis test ever
//! sees the record — against "guess one" and "build a Rust-side mark". What
//! landed instead derives the basis INSIDE the mark, at read time, from `qty`
//! and each leg's `yes_price` / `role` / `qty`: no ledger line is written, the
//! records already in the file need no backfill, and a record this engine
//! writes tomorrow is marked the second it lands. The correction route stays
//! available, and is still the right one for a basis somebody has to CORRECT
//! rather than derive.
//!
//! THE DEPENDENCY THAT PARAGRAPH NAMED IS REAL AND IS UNPAID. A basis that is
//! not merely an upper bound needs the venue FILL reports this engine does not
//! read — which is what `fees_pending: true` is naming. The derived basis
//! charges both legs at the SCHEDULE, so it is dear and the locked profit it
//! implies is conservative: the safe direction for a number an exit is screened
//! against, and still not the number the venue charged.
//!
//! # 2. The signal FLAPS. A placer needs hysteresis
//!
//! RETRACTION. This file's first draft said the loop "cannot oscillate" because
//! "`apr_bar` is monotone non-decreasing in utilization and an exit can only
//! REDUCE exposure, so the candidate set can only ever SHRINK". The first
//! clause is true. The second is false, and so is the conclusion.
//!
//!   * `RiskView::record_open` (`risk.rs:439-445`) is the ONLY mutator of
//!     `by_rel`/`by_class`, and it is `+=` only. THERE IS NO `record_close`.
//!     Within a running process `utilization()` is monotone NON-DECREASING and
//!     an exit fill cannot lower it. Exposure comes down only across a restart,
//!     through `seed_exposure_from_ledger`.
//!   * Route a future exit fill through the ordinary fill path and it reaches
//!     `fill.rs:283 rv.record_open(...)`, INCREASING exposure for an order that
//!     shrinks the position — which pushes `apr_bar` up and the candidate set
//!     out. That is a runaway, not a ratchet.
//!   * "Only a fill moves exposure" is false at the ledger level too: the
//!     ledger carries `correction` and `realized` records alongside `open` and
//!     `unwound`, and `arbbot-settle.timer` and `arbbot-hedge.timer` both
//!     mutate the fold with no engine fill involved. (An earlier draft listed
//!     `settlement` among the statuses. It is not one — NO ledger record
//!     carries `status: "settlement"`. `settlement` is a STRATEGY, and it
//!     appears on `unwound` records: it is how a basket that ran to resolution
//!     is closed out, not a fourth kind of record. Statuses and strategies are
//!     different fields and this file conflated them.)
//!   * And the INPUT oscillates on its own, with nothing of ours touching it.
//!     Over 3464 marks samples spanning 128.5 h, one basket's `maker_exit_ct`
//!     crossed the old half-cent floor 93 times — about every 83 minutes — and
//!     just under half of all tracked baskets crossed it at least once.
//!     **THE NEW FLOOR RELOCATES THAT FLAP; IT DOES NOT REDUCE IT.** Re-run the
//!     same tape against two ticks and the worst basket crosses 91 times
//!     instead of 93, and one fewer basket crosses at all. A placer's debounce
//!     must be sized against ~90 crossings over five days, NOT budgeted down on
//!     the theory that a higher floor bought quiet.
//!
//! What [`select`] is actually monotone in is the HURDLE: it is a threshold on
//! a per-basket value the hurdle does not touch, so a lower hurdle always
//! selects a subset. That — and only that — is what
//! [`tests::a_lower_hurdle_selects_a_subset_because_the_rule_is_a_threshold`]
//! proves. It cannot demonstrate an exit lowering utilization, because nothing
//! in this tree can. **A placer must debounce this signal.** Sizing that
//! debounce is the arming PR's job; this module deliberately does not pretend
//! the question is settled.
//!
//! # 3. The floor is a STALENESS SCREEN. It is not a guarantee about the exit
//!
//! RETRACTED TWICE. The first draft inherited `mark_positions.py`'s `0.005`
//! eligibility floor, which is half of one tick on a one-tick grid. The second
//! draft raised it to two ticks and argued the marks "move more than a tick
//! inside" a writer period, "so" a candidate must clear "break-even after a
//! single tick". The premise is right; the conclusion does not follow from it,
//! and the tape refutes it by an order of magnitude. The very basket this
//! file's own test calls "over the floor by more than a tick, so a tick of
//! movement does not unmake it" went `+0.0312` → `−0.0721` across ONE writer
//! period on 2026-07-29: **−10.3 ticks**. See [`MIN_EXIT_CT`], which now says
//! what the floor does and, at more length, what it does not.
//!
//! AND THE QUANTITY IT SCREENS IS NOT THE PAYOFF. `maker_exit_ct` is a snapshot
//! price of a LEGGED trade — rest a Kalshi ask, close the PM NO *when it fills*
//! — and a resting ask fills preferentially when the book is moving against it.
//! No static floor can fix that, and raising this one to ten ticks would select
//! nothing with a different number. What fixes it is a check at FILL time,
//! which is §5's fourth bullet and which nothing here does.
//!
//! # 4. The hurdle is an opportunity cost that may be unearnable
//!
//! PARTIAL RETRACTION. "The exit rule and the entry rule cannot disagree about
//! what the bar is" is true about the NUMBER and false about its safe
//! DIRECTION, which is the part that matters:
//!
//!   * `RiskView::utilization` answers a cap of zero with `1.0` —
//!     "A cap of zero reads as FULL, not empty". For an ENTRY that is
//!     fail-closed: demand the ceiling of a new quote. For an EXIT the same
//!     number pins the liquidation hurdle at its ceiling, which is maximally
//!     EAGER to sell. Fail-closed for entry is fail-open for exit, so a missing
//!     or corrupt `exec.yaml` would have this module recommend liquidating the
//!     book. [`select`] therefore refuses outright when the cap it would divide
//!     by is not a real number — fail-closed here means selecting NOTHING.
//!   * The charge is also circular at the moment it is levied. The armed engine
//!     reports its hurdle at the ceiling *because* `risk_allowed` is zero and
//!     nothing can be deployed; charging the freed capital an opportunity cost
//!     it is structurally unable to earn is not obviously the right rule. It is
//!     the rule this module implements — mirroring entry — and the arming PR
//!     owes an answer to "would the freed capital actually have been deployed",
//!     which no gauge here can supply.
//!
//! # 5. One ticker, N baskets is the NORMAL case
//!
//! [`Exit::suppress_key`] collapses every basket on a market to one
//! `(market_id, Ask)`, and the venue holds one ask per market. On the live
//! marks the most crowded Kalshi ticker carries a third of all priced baskets,
//! and five separate tickers carry more than one. So the placer must:
//!
//!   * AGGREGATE the N baskets behind a ticker into one resting ask, and SPLIT
//!     a single partial fill back across N `closes_ts` records — for which
//!     nothing in these types has anywhere to put the split;
//!   * RECORD that an exit is outstanding. Exits do not reserve (see
//!     [`tests::routing_an_exit_through_the_opening_cap_would_deadlock_the_cap`])
//!     and nothing else marks one either: the ledger record stays
//!     `status: "open"` until the unwind fill books, so the next scan
//!     re-selects the same basket and a naive placer rests a SECOND ask.
//!     `Engine::unwind_seen` gates only the log line;
//!   * PULL THE ENTRY QUOTE FIRST. `scripts/unwind_positions.py:45-49` records
//!     what happens otherwise: "self-trade prevention deadlocks the unwind IOC"
//!     — card ed6a5910, 14 soft unwinds blocked. That is what
//!     [`Exit::suppress_key`] is for, and it is computed and NOT installed here
//!     because pulling the entry quote off a side and resting nothing in its
//!     place is strictly worse than the status quo;
//!   * RE-PRICE AT FILL TIME, against the book WITHOUT our own quotes, AND
//!     ABANDON THE CLOSE IF IT NO LONGER PAYS. Nothing above does this, and
//!     nothing in this module does either: [`MIN_EXIT_CT`] is checked once, at
//!     SCAN time, against a snapshot that is up to one writer period old, and
//!     the PM leg is then taken at whatever exists when the Kalshi ask fills —
//!     preferentially the moment the book has moved against it. The frozen
//!     Python ALREADY implements the missing half; `unwind_positions.py:80-83`
//!     is the whole requirement in three lines: "RE-PRICE against the book
//!     WITHOUT our own quotes … Only unwind if it is still profitable against
//!     the real residual book." An arming author who reads §5 as a complete
//!     list rests an ask, takes the PM leg at fill, and has performed no floor
//!     check anywhere near the trade.
//!
//! # The rule itself
//!
//! THE ENTRY RULE IS "cross when the edge beats the APR bar". The symmetric
//! EXIT rule is "close when holding is worse than redeploying" — unwind a
//! basket whose remaining forward APR is below the maker hurdle that the freed
//! capital would itself have to clear. Both halves already existed:
//!
//!   * the hurdle is `crate::apr_bar(utilization())`, the number
//!     `Engine::apr_tick` already installs on every quoter and reports as
//!     `maker_apr_bar`. The caller passes the bar IN FORCE, so the exit is
//!     measured against the same number a fresh quote is — subject to §4 above;
//!   * the forward APR is `forward_hold_apr` from `data/exec/marks.json`
//!     (`scripts/mark_positions.py`): remaining locked profit over the
//!     liquidation value the position is tying up, annualised to resolution.
//!
//! WHY THERE IS NO PLACING CODE HERE. `Intent` has four variants — place,
//! cancel, hedge-needed, skip — and an exit is not expressible as any of them:
//! a fill on a tagged place mints a hedge that OPENS the other leg, and
//! `engine::fill::book_basket` appends `status: "open"`. An exit fill has to
//! CLOSE the other leg and append a `status: "unwound"` record netting against
//! one specific open record's `ts` (`ledger::open_exposure`). None of that is
//! expressible here, so it was built somewhere else: [`crate::maker_exit`] is
//! the placer, off its own order path and its own ledger write. §1's condition
//! on building one — "not until the recycler and the engine can see the same
//! book" — is met as of 2026-08-14, in both directions.
//!
//! THE DIRECTION IS FIXED BY THE INPUT. `maker_exit_ct` is priced by
//! `mark_positions.py` for the STANDARD basket only (long Kalshi YES + long PM
//! NO): rest a Kalshi ask one tick inside the competing ask, and close the PM
//! NO one tick through its ask when it fills. Inverted baskets (Kalshi NO + PM
//! YES) get `maker_exit_ct: null` there, so they are unpriceable here and are
//! never selected. That is the scope, not an oversight.
//!
//! AND IT NOW CARRIES WEIGHT IT DID NOT CARRY BEFORE. `marks::compute_row`
//! reads that direction through the ONE side-vocabulary table
//! (`naked_act::Held`), which knows this engine spells a Kalshi short
//! `side:"ask"` — so the engine-written baskets that became visible on
//! 2026-08-14 are classified honestly, and the inverted ones among them
//! publish `maker_exit_ct: null` and are refused here as [`Skip::NotPriceable`]
//! before anything prices them. That refusal is the SELECTION-level half of a
//! belt-and-braces pair; the other half is `naked_act::worst_lot`, which
//! refuses a lot pointing the wrong way when the placer asks for its basis.
//!
//! THE MARKS ARE READ TWICE PER STATS TICK — once by `stats_tick` for the
//! take-take bar and once by `Engine::unwind_tick` — and the file is rewritten
//! every two minutes by `arbbot-marks.timer`. The first draft said a stale
//! take-take bar alongside fresh unwind marks "is not a state that can exist".
//! Across two separate `read_to_string` calls straddling a rewrite it can, for
//! one tick. Both reads apply the same staleness rule, so neither can act on an
//! OLD file; what they can briefly disagree about is which of two adjacent
//! files they read.
//!
//! DATES ARE UTC HERE AND LOCAL IN THE WRITER. `today_iso` is UTC
//! (`resolve.rs:105-107`); `mark_positions.py:100` compares against
//! `datetime.date.today()`, which is local. Between 20:00 and 24:00 EDT this
//! module's "today" is already the writer's tomorrow, so the [`Skip::Settled`]
//! guard fires up to four hours EARLY relative to the forward APRs it is
//! reading. That is the conservative direction — skip sooner, never later — so
//! it is documented rather than corrected.

use crate::taketake::{bar_from_marks, today_iso, Bar};
use arb_core::model::BookSide;

/// The price increment on both legs. Kalshi quotes whole cents and
/// `mark_positions.py` steps PM in `Decimal("0.01")` on both sides of the exit
/// (`k_ask - 0.01`, `p_ask + 0.01`), so this is the resolution of the input,
/// not a modelling choice.
const TICK_CT: f64 = 0.01;

/// Minimum per-contract profit a maker exit must SHOW before it is worth
/// reporting: TWO TICKS.
///
/// THIS IS A SCREEN AGAINST A STALE SNAPSHOT. IT IS NOT A GUARANTEE ABOUT
/// REALIZED EXIT VALUE, and the quantity it screens is not the payoff. Both
/// halves of that sentence are corrections to the previous draft, which claimed
/// two ticks bought "break-even after a single adverse tick".
///
/// WHAT THE TWO TICKS ARE FOR. `maker_exit_ct` is
/// `k_exit + (1 - p_close) - basis - fees` with unit coefficients on both
/// touches, so `∂mx/∂k_ask = +1` and `∂mx/∂p_ask = −1`: one adverse tick on
/// EITHER leg costs a full [`TICK_CT`]. The old floor — inherited from
/// `mark_positions.py`'s `mx >= Decimal("0.005")` eligibility test — was half
/// of one tick, i.e. finer than the grid its own input is quoted on, so it
/// admitted candidates that were inside their input's resolution before they
/// were ever read. Two ticks buys exactly two things:
///
///   * ONE TICK FOR A KNOWN, DIRECTIONAL OVERSTATEMENT. `mark_positions.py:227`
///     prices `k_exit_px`/`p_close_px` from `kalshi_books`/`pmus_topbook` with
///     NO self-exclusion, while the entry side explicitly excludes our own
///     resting size (`quoter.rs:310`, "our resting subtracted"). On the PM leg
///     that bias only ever flatters: `p_close_px = p_ask + tick` takes THROUGH
///     an ask our own resting size may be the touch of. ONE TICK IS A LOWER
///     BOUND ON THAT BIAS, NOT A BOUND ON IT — on a book where our resting size
///     is the only ask, excluding it may leave no ask at all, which is the
///     thin-book case `unwind_positions.py:80-83` exists for ("on thin books
///     our maker quotes ARE the touch … flattered by ourselves"). So this tick
///     does not neutralise the bias; it pays the smallest instalment the grid
///     can express, on a debt whose size the frozen Python cannot tell us.
///   * ONE TICK so the candidate is not inside the resolution of its own input.
///
/// WHAT THE TWO TICKS ARE NOT FOR, AND THE TAPE SAYS SO. They do not make a
/// candidate break-even after an adverse move. Over 128.5 h of
/// `marks_history.jsonl` (3464 samples, ~92k adjacent-period deltas) an adverse
/// move of a tick or more is rare — 0.9% of deltas — and LARGE when it happens:
/// 55% of those are two ticks or worse, 41% are three or worse, and the worst
/// single writer period in the tape is −35 ticks. The 2026-07-29 candidate this
/// file's [`tests::a_candidate_within_one_tick_of_the_floor_is_flagged_as_noise`]
/// uses as its example of a basket WITH room went `+0.0312` → `−0.0721` in one
/// 121-second period: −10.3 ticks, from a level a tick clear of this floor. A
/// floor sized to survive that would select nothing at all.
///
/// AND THE NUMBER IT SCREENS IS NOT THE PAYOFF. `maker_exit_ct` prices a LEGGED
/// trade at one instant: the Kalshi ask RESTS, and the PM NO is taken WHEN IT
/// FILLS. A resting ask fills preferentially when the book is coming to meet
/// it, i.e. when the PM close has already moved against us, so the realized
/// exit is adversely selected relative to this number by construction. The
/// floor guards a snapshot at SCAN time; what would guard the trade is a
/// re-price at FILL time, and that is §5's fourth bullet, and it does not exist
/// here.
///
/// THE FEE TERM IS NOT SMOOTH, either. `curves.py` applies `ceil_cents` to the
/// Kalshi MAKER leg's TOTAL, so per contract the fee steps by `$0.01/qty` — a
/// full tick at `qty=1`. The unit-coefficient derivation above is exact between
/// steps and off by one step across one; it is a good guide to the size of this
/// floor, not an identity.
///
/// A candidate that clears this by less than a tick is reported as
/// [`Exit::near_floor`]: noise even by the weak reading above.
const MIN_EXIT_CT: f64 = 2.0 * TICK_CT;

/// [`MIN_EXIT_CT`] as an exact-decimal literal, for the callers that compare in
/// the decimal context rather than in `f64` — `maker_exit`'s cross test, which
/// must hold a cross to the same floor `consider` admitted the lot under.
///
/// Two spellings of one number, so `min_exit_ct_spellings_agree` pins them
/// together: a floor that drifted between the selector and the thing it gates
/// would spend the spread to reach a trade the selector had already refused.
pub const MIN_EXIT_CT_S: &str = "0.02";

/// A basket that should be quoted out of: holding it is worse than redeploying
/// its capital, and the passive exit is profitable today.
#[derive(Debug, Clone, PartialEq)]
pub struct Exit {
    pub rel_id: String,
    /// The Kalshi ticker the exit ask would rest on.
    pub market_id: String,
    /// The `ts` of the `open` ledger record this closes. THE BASKET IDENTITY:
    /// one relationship holds SEVERAL independently-priced positions, entered
    /// at different prices on different days and converging at different rates,
    /// and `ledger::open_exposure` nets an unwind against exactly one of them
    /// by `(relationship_id, closes_ts)`. A candidate that named only the
    /// relationship would be unbookable.
    pub opened_ts: f64,
    pub qty: i64,
    /// Remaining forward APR of CONTINUING to hold, %/yr.
    pub fwd_apr: f64,
    /// Per-contract profit the maker exit locks, net of both legs' fees.
    pub exit_ct: f64,
    /// Per-contract profit CROSSING both legs would lock, estimated from the
    /// liquidation mark: `mark_pnl_usd / qty` less the two conservative ticks
    /// `maker_exit`'s cross prices with (a tick under the Kalshi bid, a tick
    /// through the PM-US ask).
    ///
    /// AN ORDERING SIGNAL, NOT A DECISION. `maker_exit::decide` re-prices the
    /// cross against a live quote and grades it itself; this only has to be good
    /// enough to answer "would this one convert NOW, or sit?" — because those
    /// two candidates are worth wildly different amounts of the ONE exit slot.
    /// A candidate that crosses is done inside a cycle; one that can only rest
    /// holds the slot for `MAX_RESTING_S` and, on this book, converts at
    /// approximately zero.
    pub cross_ct: f64,
    /// Whether THIS process could act on it: the relationship is inside the
    /// `--rel-prefix` scope this engine loaded quoters for. See §1 — this read
    /// `false` for every candidate when the armed process ran three families,
    /// and `maker_exit`'s §1 re-derived it on 2026-07-31 under
    /// `--rel-prefix xvus-`, where the candidate set and the owned set are the
    /// same set. The test is enforced either way rather than assumed.
    pub actionable: bool,
    /// Clears [`MIN_EXIT_CT`] by less than one [`TICK_CT`]. Reported so the
    /// operator can tell a decision from a coin flip: one tick of book movement
    /// puts this basket back below the floor.
    pub near_floor: bool,
    /// `mark_positions.py`'s own flag that the resolve date is a CURATED GUESS
    /// (`resolve_dates.py`: "event date not officially fixed"). Both halves of
    /// the decision rest on it — `forward_hold_apr` is annualised to that date,
    /// and [`Skip::Settled`] compares against it — so a candidate carrying it
    /// is a candidate whose APR is speculative.
    ///
    /// THE EXPOSURE IS THE WHOLE BOOK, NOT PART OF IT. On the 2026-07-29 marks
    /// EVERY priced position carries this flag. It is surfaced rather than
    /// refused on for exactly that reason — refusing would select nothing,
    /// ever — but "surfaced" must not be read as "bounded".
    ///
    /// AND THERE IS NO GUARD BEHIND IT. An earlier draft said "the guard that
    /// does NOT rest on the guess is [`Skip::NotPriceable`]: a resolved market
    /// has no book". That is false on the Kalshi leg, for a reason that is in
    /// the frozen Python and cannot be fixed here — see [`Skip::NotPriceable`].
    pub resolves_estimated: bool,
}

impl Exit {
    /// The (market, side) an exit quote owns, for `Quoter::set_suppress`.
    ///
    /// ALWAYS THE KALSHI ASK. We are long Kalshi YES, so the passive exit rests
    /// an ask on that market — and that is also the side the ENTRY quoter uses
    /// to open an inverted basket (`maker_leg_indices` returns both legs and
    /// `target` is asked for both sides), so the two collide unless the entry
    /// quoter steps out. That collision is why `suppress` exists.
    ///
    /// NOT UNIQUE PER BASKET, and §5 is about the consequences: several
    /// candidates on one ticker collapse to one key and to one venue-side ask.
    pub fn suppress_key(&self) -> (String, BookSide) {
        (self.market_id.clone(), BookSide::Ask)
    }
}

/// Why a position is not a candidate. Reported rather than dropped: a basket
/// that ALMOST cleared is the interesting telemetry, and "nothing selected" has
/// five very different causes.
#[derive(Debug, Clone, PartialEq)]
pub enum Skip {
    /// No live book behind it, so no exit price and no forward APR.
    ///
    /// THIS HOLDS ON THE PM LEG ONLY, AND AN EARLIER DRAFT CLAIMED IT HELD ON
    /// BOTH. `pmus_topbook` returns `None` for a missing quote, so a dark PM
    /// leg does produce a null row and does land here. The Kalshi leg does not:
    /// `mark_positions.py:70-71` reads
    /// `Decimal(str(m.get("yes_bid_dollars") or 0))`, and that `or 0` turns a
    /// MISSING quote into `Decimal("0")` — which is not `None`. So the row
    /// passes `mark_positions.py:222`'s `k_ask is not None` test, enters the
    /// maker-exit branch, computes `k_exit_px = k_ask - 0.01 = -0.01`, and
    /// hands that to `leg_fee`. `FeeSchedule.fee` (`curves.py:85`) rejects any
    /// price outside `[0, 1]` with `ValueError("bad fill: …")`, which
    /// propagates out of an unguarded loop in `main()` and kills the run BEFORE
    /// the file is written.
    ///
    /// SO WHAT PROTECTS THIS MODULE IS THE CLOCK, AND FOR FIFTEEN MINUTES IT
    /// DOES NOT. The writer dies, `marks.json` FREEZES at its last good version
    /// — which still carries the now-resolved market as a live, fully-priced
    /// candidate — and the scan runs against that frozen file until
    /// `taketake::MAX_MARKS_AGE_S` (900 s) ages it out and [`select`] refuses.
    /// Inside that window the only thing that can catch it is [`Skip::Settled`],
    /// which compares against a date that is a guess for every priced position
    /// in the book. The Python is frozen and this cannot be fixed here; what
    /// follows for a placer is that a priced row is not evidence of a live
    /// book, and the venue must be asked before an order is rested.
    NotPriceable,
    /// At or past its resolve date — settling or settled.
    ///
    /// Inside the fifteen-minute window above this is not a belt to
    /// `NotPriceable`'s braces, it is the ONLY strap — and it is only as good
    /// as the date. When `resolves_estimated` is set that date is a curated
    /// guess, and a guess that runs LATE lets this pass a market that has
    /// already resolved. On the live book every priced position sets that flag.
    Settled { resolves_by: String },
    /// A family whose exits this engine does not own. Sports baskets resolve in
    /// hours and `mark_positions.py` keeps every unwind signal dark for them
    /// ("hold to settlement — the sweeper realizes them"); contradicting the
    /// writer of our own input is not a decision this module gets to make.
    HeldToSettlement,
    /// Holding still beats redeploying.
    HoldIsBetter { fwd_apr: f64, hurdle: f64 },
    /// Holding is the worse trade, but getting out costs more than it frees.
    /// THE NEAR-MISS POPULATION, and the most useful number this module
    /// produces: it carries `qty` so the report can say what forcing this set
    /// out would cost rather than only how many there are.
    ExitUnprofitable { exit_ct: f64, qty: i64 },
}

/// A scan's skips, collapsed for the log line and the gauges.
///
/// Without this every one of the five causes above renders as
/// `unwind_candidates: 0` and an operator cannot tell "nothing has converged"
/// from "the marks writer is holding this whole book dark".
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SkipTally {
    pub not_priceable: usize,
    pub settled: usize,
    pub held_to_settlement: usize,
    pub hold_is_better: usize,
    /// Cleared the APR test and failed ONLY the exit floor.
    pub exit_unprofitable: usize,
    /// ...and what forcing that population out at today's marks would cost, in
    /// dollars. Negative. This is the number that says the APR test alone is
    /// not a strategy.
    pub exit_unprofitable_usd: f64,
}

impl SkipTally {
    pub fn of(skips: &[Skip]) -> Self {
        let mut t = Self::default();
        for s in skips {
            match s {
                Skip::NotPriceable => t.not_priceable += 1,
                Skip::Settled { .. } => t.settled += 1,
                Skip::HeldToSettlement => t.held_to_settlement += 1,
                Skip::HoldIsBetter { .. } => t.hold_is_better += 1,
                Skip::ExitUnprofitable { exit_ct, qty } => {
                    t.exit_unprofitable += 1;
                    t.exit_unprofitable_usd += exit_ct * *qty as f64;
                }
            }
        }
        t
    }

    /// The tally's INTEGER shape, for edge-triggering a report on.
    ///
    /// EXCLUDES `exit_unprofitable_usd` deliberately: that is a price, it moves
    /// on every book tick, and triggering on it would make every scan "new" and
    /// the report a paragraph a minute that nobody reads. The counts move only
    /// when a basket changes CAUSE — which is precisely the change
    /// [`Self::describe`] exists to announce.
    pub fn counts(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.not_priceable,
            self.settled,
            self.held_to_settlement,
            self.hold_is_better,
            self.exit_unprofitable,
        )
    }

    /// One line. Every cause, because four of the five render as
    /// `unwind_candidates: 0` and they are not the same finding.
    pub fn describe(&self) -> String {
        format!(
            "unpriceable {} settled {} held-to-settlement {} hold-is-better {} \
             exit-unprofitable {} (forcing them out costs ${:.2})",
            self.not_priceable,
            self.settled,
            self.held_to_settlement,
            self.hold_is_better,
            self.exit_unprofitable,
            self.exit_unprofitable_usd
        )
    }
}

/// One position's verdict. Pure: the caller supplies the hurdle, the ownership
/// scope and the day.
fn consider(
    p: &serde_json::Value,
    hurdle: f64,
    owned: &[String],
    today: &str,
) -> Result<Exit, Skip> {
    let rel_id = p.get("relationship_id").and_then(|v| v.as_str()).unwrap_or_default();
    if rel_id.starts_with("sports-") {
        return Err(Skip::HeldToSettlement);
    }
    let resolves_by = p.get("resolves_by").and_then(|v| v.as_str()).ok_or(Skip::NotPriceable)?;
    // String compare, because both are `YYYY-MM-DD` and that ordering IS the
    // date ordering. `<=` and not `<`: a market resolving today is settling.
    if resolves_by <= today {
        return Err(Skip::Settled { resolves_by: resolves_by.to_string() });
    }
    let (Some(fwd_apr), Some(exit_ct), Some(market_id), Some(opened_ts), Some(qty)) = (
        p.get("forward_hold_apr").and_then(|v| v.as_f64()),
        p.get("maker_exit_ct").and_then(|v| v.as_f64()),
        p.get("kalshi_ticker").and_then(|v| v.as_str()),
        p.get("ts").and_then(|v| v.as_f64()),
        p.get("qty").and_then(|v| v.as_i64()),
    ) else {
        return Err(Skip::NotPriceable);
    };
    if qty < 1 {
        return Err(Skip::NotPriceable);
    }
    // THE RULE. Holding beats redeploying while the remaining forward APR still
    // clears the bar a fresh maker quote would have to clear.
    if fwd_apr >= hurdle {
        return Err(Skip::HoldIsBetter { fwd_apr, hurdle });
    }
    if exit_ct < MIN_EXIT_CT {
        return Err(Skip::ExitUnprofitable { exit_ct, qty });
    }
    // The liquidation mark IS the cross, at the touch: `marks` prices it by
    // selling the Kalshi YES at its bid and buying the PM-US YES at its ask,
    // charging BOTH takers — the same two legs, the same two roles. Subtracting
    // the two conservative ticks `maker_exit` crosses with turns it into that
    // module's own number without this file having to know its pricing.
    let cross_ct = p
        .get("mark_pnl_usd")
        .and_then(serde_json::Value::as_f64)
        .map_or(f64::NEG_INFINITY, |m| m / qty as f64 - 2.0 * TICK_CT);
    Ok(Exit {
        rel_id: rel_id.to_string(),
        market_id: market_id.to_string(),
        opened_ts,
        qty,
        fwd_apr,
        exit_ct,
        cross_ct,
        // `main.rs:387`'s rule, exactly: no prefixes means the process loaded
        // every relationship in the registry, so it owns them all.
        actionable: owned.is_empty() || owned.iter().any(|x| rel_id.starts_with(x.as_str())),
        near_floor: exit_ct < MIN_EXIT_CT + TICK_CT,
        resolves_estimated: p
            .get("resolves_estimated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    })
}

/// Every open position marks can price, against the hurdle in force.
///
/// `Err` is a REFUSAL to decide anything at all, not an empty answer. Three
/// things can force it, and all three are fail-CLOSED — selecting nothing:
///
///   1. `global_cap_usd` is not a positive, finite number. That is the
///      denominator `RiskView::utilization` divides by, and it
///      answers a degenerate one with `1.0` — the CEILING hurdle, maximally
///      eager to liquidate. See §4: fail-closed for entry is fail-open for
///      exit, so the exit side must refuse rather than inherit the number.
///   2. `hurdle` is outside the band `crate::apr_bar` can produce. A bar that
///      did not come from the utilization curve is a bar this rule cannot
///      interpret — including the `0.0` a bench/no-risk run installs.
///   3. Marks that are stale, unageable or corrupt. The rule is
///      `taketake::bar_from_marks`'s and not a second copy of it — one file,
///      one definition of "too old to act on".
///
/// A file that is simply ABSENT is a cold start (`Bar::NoPortfolio`), which
/// selects nothing and is not an error.
///
/// `owned` is the process's `--rel-prefix` scope; it does not filter, it
/// annotates [`Exit::actionable`] (§1).
pub fn select(
    marks_json: &str,
    hurdle: f64,
    global_cap_usd: f64,
    owned: &[String],
    now: f64,
) -> Result<(Vec<Exit>, Vec<Skip>), String> {
    if !global_cap_usd.is_finite() || global_cap_usd <= 0.0 {
        return Err(format!(
            "global cap is {global_cap_usd} — utilization() answers a degenerate cap with 1.0, \
             which pins the exit hurdle at its CEILING. Refusing to select."
        ));
    }
    if !hurdle.is_finite() || !(crate::APR_FLOOR..=crate::APR_CEIL).contains(&hurdle) {
        return Err(format!(
            "hurdle {hurdle} is outside [{}, {}] — not a bar apr_bar() can produce. \
             Refusing to select.",
            crate::APR_FLOOR,
            crate::APR_CEIL
        ));
    }
    if let Bar::Untrusted { why } = bar_from_marks(marks_json, now) {
        return Err(why);
    }
    let today = today_iso(now);
    let mut exits = Vec::new();
    let mut skips = Vec::new();
    let doc: serde_json::Value = serde_json::from_str(marks_json).unwrap_or_default();
    let empty = Vec::new();
    for p in doc.get("positions").and_then(|v| v.as_array()).unwrap_or(&empty) {
        match consider(p, hurdle, owned, &today) {
            Ok(e) => exits.push(e),
            Err(s) => skips.push(s),
        }
    }
    // CAN IT CONVERT NOW, THEN deepest lock first.
    //
    // The old order was `(qty desc, fwd_apr asc)` alone, and it predates the
    // cross entirely: it cannot tell a candidate that executes inside one cycle
    // from one that will hold the single `MAX_RESTING` slot for an hour and
    // convert at approximately zero. On the live book of 2026-08-25 that put
    // FIVE rest-only lots at the head of the queue — `KXNOBELPEACE-26-DJT`
    // resting behind 6,551 lots at the touch — while `KXTIME-26-POP` at +0.0240
    // and `KXBRPRES-26-FBOL` at +0.0545 waited behind them for a slot that turns
    // over once an hour. The slot is the scarce resource; spending it on the
    // candidate that cannot use it is the whole of what this fixes.
    //
    // Only the GROUPING is new. Within each group the old comparator is
    // untouched, so nothing about the relative order of two rest-only candidates
    // — or of two crossable ones — has moved.
    //
    // `cross_ct` is an estimate off the marks mark; `maker_exit::decide` still
    // re-prices against a live quote and grades the cross itself. Being wrong
    // here costs an ordering, not a trade.
    //
    // Still a DISPLAY order and still not stable under ties — see
    // [`identity_set`] for the thing that must be.
    exits.sort_by(|a, b| {
        let (ca, cb) = (a.cross_ct >= MIN_EXIT_CT, b.cross_ct >= MIN_EXIT_CT);
        cb.cmp(&ca).then(b.qty.cmp(&a.qty)).then(a.fwd_apr.total_cmp(&b.fwd_apr))
    });
    Ok((exits, skips))
}

/// The candidate SET, canonically ordered, for edge-triggering a report on.
///
/// NOT the display order. `select` sorts by `(qty desc, fwd_apr asc)` and the
/// live book routinely carries several baskets that tie on BOTH — four
/// identical 10-lot `xvus-france-pres-27-brunoretailleau` baskets sat at the
/// same rounded forward APR on 2026-07-29. Any reordering of a tie makes the
/// display vector compare unequal, and an edge-trigger keyed on it would print
/// the same paragraph again. `(rel_id, opened_ts)` is unique per basket, so
/// sorting it gives a set that changes only when the SET changes.
pub fn identity_set(exits: &[Exit]) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> =
        exits.iter().map(|e| (e.rel_id.clone(), e.opened_ts.to_bits())).collect();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    /// The two spellings of the exit floor are one number.
    #[test]
    fn min_exit_ct_spellings_agree() {
        assert_eq!(
            super::MIN_EXIT_CT_S.parse::<f64>().expect("a decimal literal"),
            super::MIN_EXIT_CT,
            "MIN_EXIT_CT_S must be MIN_EXIT_CT, or the selector and the cross gate disagree"
        );
    }

    use super::*;

    /// 2026-07-29T20:15:04Z, the live marks file's own `generated_at`.
    const NOW: f64 = 1785356104.0;
    /// A cap that is merely REAL. Its size decides nothing here — only
    /// `hurdle` does — so it is an arbitrary positive number.
    const CAP: f64 = 100.0;

    fn iso_z(t: f64) -> String {
        let s = (t as i64).rem_euclid(86_400);
        format!("{}T{:02}:{:02}:{:02}Z", today_iso(t), s / 3600, (s % 3600) / 60, s % 60)
    }

    /// A marks document around `positions`, stamped fresh at [`NOW`].
    fn marks(positions: &str) -> String {
        format!(r#"{{"generated_at":"{}","positions":[{positions}]}}"#, iso_z(NOW))
    }

    /// One position in the shape `mark_positions.py` writes.
    fn pos(rel: &str, qty: i64, resolves: &str, fwd: &str, mx: &str) -> String {
        format!(
            r#"{{"relationship_id":"{rel}","ts":1784646659.716,"kalshi_ticker":"KX-{rel}",
                 "qty":{qty},"cost_usd":10.0,"locked_profit_usd":1.0,
                 "resolves_by":"{resolves}","resolves_estimated":true,
                 "forward_hold_apr":{fwd},"maker_exit_ct":{mx}}}"#
        )
    }

    /// `pos`, plus the liquidation mark `cross_ct` is derived from. The plain
    /// `pos` omits it deliberately: every test written before the cross gets
    /// `NEG_INFINITY`, is therefore not crossable, and keeps asserting the order
    /// it was written about.
    fn pos_mark(
        rel: &str,
        qty: i64,
        resolves: &str,
        fwd: &str,
        mx: &str,
        mark: &str,
    ) -> String {
        format!(
            r#"{{"relationship_id":"{rel}","ts":1784646659.716,"kalshi_ticker":"KX-{rel}",
                 "qty":{qty},"cost_usd":10.0,"locked_profit_usd":1.0,
                 "resolves_by":"{resolves}","resolves_estimated":true,
                 "forward_hold_apr":{fwd},"maker_exit_ct":{mx},"mark_pnl_usd":{mark}}}"#
        )
    }

    /// **A CANDIDATE THAT CAN CONVERT NOW OUTRANKS A BIGGER ONE THAT CANNOT.**
    ///
    /// There is ONE exit slot. A crossable candidate is done inside a cycle; a
    /// rest-only one holds the slot for `MAX_RESTING_S` and, on this book,
    /// converts at approximately zero — our ask sat behind 6,551 lots at the
    /// touch on `dontru` and 711 on `popleo`. Ordering by size alone spends the
    /// scarce resource on the candidate that cannot use it, which is exactly
    /// what the live book did on 2026-08-25: five rest-only lots at the head of
    /// the queue while `KXTIME-26-POP` at +0.0240 and `KXBRPRES-26-FBOL` at
    /// +0.0545 waited an hour each for a turn.
    ///
    /// `big` is twice the size and would sort first under the old comparator.
    #[test]
    fn a_crossable_candidate_outranks_a_bigger_one_that_can_only_rest() {
        let big = pos_mark("big", 10, "2027-01-01", "5.0", "0.05", "0.10");
        let quick = pos_mark("quick", 5, "2027-01-01", "5.0", "0.05", "0.50");
        let m = marks(&format!("{big},{quick}"));
        let (exits, _) = sel(&m, 16.0).expect("both are candidates");
        assert_eq!(exits.len(), 2);
        assert_eq!(exits[0].rel_id, "quick", "the one that converts now goes first");
        assert!(exits[0].cross_ct >= MIN_EXIT_CT, "and it really is crossable: {:?}", exits[0]);
        assert!(exits[1].cross_ct < MIN_EXIT_CT, "while the bigger one is rest-only");
    }

    /// ...AND WITHIN A GROUP NOTHING MOVED. Two rest-only candidates still order
    /// by `(qty desc, fwd_apr asc)`, which is the comparator every test written
    /// before the cross depends on.
    #[test]
    fn two_rest_only_candidates_keep_the_old_order() {
        let small = pos_mark("small", 5, "2027-01-01", "5.0", "0.05", "0.10");
        let big = pos_mark("big", 10, "2027-01-01", "5.0", "0.05", "0.10");
        let m = marks(&format!("{small},{big}"));
        let (exits, _) = sel(&m, 16.0).expect("both are candidates");
        assert_eq!(exits[0].rel_id, "big", "neither crosses, so size still leads");
    }

    /// `select` with a real cap and no ownership scope.    /// `select` with a real cap and no ownership scope.
    fn sel(m: &str, hurdle: f64) -> Result<(Vec<Exit>, Vec<Skip>), String> {
        select(m, hurdle, CAP, &[], NOW)
    }

    /// THE RULE, both directions, on the shape the live book carried when this
    /// was written.
    ///
    /// A long-dated basket sitting at forward 12.6%/yr with a maker exit
    /// locking 3.12c/ct, against a maker hurdle of `apr_bar(1.0)` = 16.0%/yr —
    /// which is the bar a book at its class cap asks for, and the book was at
    /// its cap. Holding earns 12.6%/yr on capital a fresh quote must make 16%
    /// on: redeploying is the better trade and the position is a candidate.
    /// Drop the hurdle under its forward APR and the same position is held.
    #[test]
    fn a_basket_is_exited_when_holding_earns_less_than_the_bar_the_freed_capital_faces() {
        let m = marks(&pos("longdated", 26, "2027-04-25", "12.6", "0.0312"));

        let (exits, _) = sel(&m, 16.0).expect("fresh marks");
        assert_eq!(exits.len(), 1, "12.6%/yr does not clear a 16%/yr hurdle: {exits:?}");
        assert_eq!(exits[0].qty, 26);
        assert_eq!(exits[0].rel_id, "longdated");
        assert_eq!(exits[0].opened_ts, 1784646659.716, "the basket identity, not just the rel");

        let (held, skips) = sel(&m, 12.0).expect("fresh marks");
        assert!(held.is_empty(), "12.6%/yr beats a 12%/yr hurdle — hold: {held:?}");
        assert!(
            matches!(skips.as_slice(), [Skip::HoldIsBetter { .. }]),
            "and it says so: {skips:?}"
        );

        // the boundary: equal is HOLD. An exit has to be strictly better.
        assert!(sel(&m, 12.6).unwrap().0.is_empty(), "equal is not better");
    }

    /// AND THE EXIT MUST ITSELF PAY, BY TWO TICKS. The commonest shape in the
    /// live book on 2026-07-29: a long-dated basket at forward 11.0%/yr — under
    /// every live hurdle — whose maker exit priced at MINUS 3.3c/contract,
    /// because the passive exit does not recover the entry basis plus both
    /// legs' fees. Selecting on the APR test alone would quote out of it and
    /// PAY to free the capital.
    ///
    /// The floor is [`MIN_EXIT_CT`] = two [`TICK_CT`]s. The numbers below are
    /// live ones that show the OLD floor was finer than its own input: a
    /// TIME-POTY basket priced +1.44c/ct, cleared the half-cent floor with
    /// room, and was under water shortly after.
    ///
    /// THEY DO NOT SHOW THE NEW FLOOR SURVIVING ANYTHING. On the same tape a
    /// basket at +3.12c/ct — a tick clear of this floor — lost 10.3 ticks in a
    /// single writer period. What this test pins is that the floor is sized to
    /// the grid, not that a candidate clearing it is safe; see [`MIN_EXIT_CT`].
    #[test]
    fn a_maker_exit_must_clear_two_ticks_because_one_adverse_tick_costs_one() {
        let m = marks(&pos("unconverged", 26, "2027-04-25", "11.0", "-0.033"));
        let (exits, skips) = sel(&m, 16.0).expect("fresh marks");
        assert!(exits.is_empty(), "a 3.3c/ct loss is not a profit-take: {exits:?}");
        assert!(
            matches!(skips.as_slice(), [Skip::ExitUnprofitable { qty: 26, .. }]),
            "and the near-miss carries its size, so the report can price it: {skips:?}"
        );

        // the old floor: half a tick on a one-tick grid, i.e. finer than the
        // resolution of the number it screened. 1.44c/ct cleared it.
        let old_floor = marks(&pos("timeai", 4, "2026-12-09", "15.6", "0.0144"));
        assert!(
            sel(&old_floor, 16.0).unwrap().0.is_empty(),
            "1.44c/ct is one adverse tick from a loss — not a decision"
        );
        // ...and one tick is still not enough: it is entirely spent on the PM
        // self-pricing bias, which is always adverse and of which a tick is a
        // LOWER BOUND, not a bound.
        let one_tick = marks(&pos("onetick", 10, "2027-04-25", "11.0", "0.0100"));
        assert!(sel(&one_tick, 16.0).unwrap().0.is_empty(), "one tick does not clear the floor");

        let ok = marks(&pos("ok", 10, "2027-04-25", "11.0", "0.0200"));
        assert_eq!(sel(&ok, 16.0).unwrap().0.len(), 1, "the floor itself qualifies");
    }

    /// A CANDIDATE INSIDE ONE TICK OF THE FLOOR IS NOISE, AND SAYS SO.
    ///
    /// This pins the FLAG and nothing more. A 2.0c/ct exit is one tick from the
    /// floor and a 3.12c/ct one is not, so the report can tell them apart —
    /// which is all `near_floor` claims. It is NOT a claim that the unflagged
    /// one is durable; see the second block.
    #[test]
    fn a_candidate_within_one_tick_of_the_floor_is_flagged_as_noise() {
        let thin = marks(&pos("thin", 10, "2027-04-25", "11.0", "0.0200"));
        let e = sel(&thin, 16.0).unwrap().0;
        assert!(e[0].near_floor, "2.0c/ct is one tick from the floor: {e:?}");

        // The live selected candidate, +3.12c/ct: over the floor by more than a
        // tick, so the flag is off. AN EARLIER DRAFT ADDED "so a tick of
        // movement does not unmake it", and this exact basket refutes it: it
        // went +0.0312 -> -0.0721 across one 121-second writer period on
        // 2026-07-29, which is -10.3 ticks. `near_floor` being false means the
        // candidate is outside its input's resolution, NOT that it survives the
        // book. See MIN_EXIT_CT.
        let real = marks(&pos("melenchon", 26, "2027-04-25", "12.6", "0.0312"));
        let e = sel(&real, 16.0).unwrap().0;
        assert!(!e[0].near_floor, "3.12c/ct has a tick of room: {e:?}");
    }

    /// A DEGENERATE CAP REFUSES TO SELECT ANYTHING.
    ///
    /// `RiskView::utilization` answers `cap <= 0` with `1.0` — "A cap of zero
    /// reads as FULL, not empty" — which is fail-closed for an ENTRY and
    /// fail-OPEN for an exit: it pins the liquidation hurdle at its ceiling, so
    /// a missing or corrupt `exec.yaml` would recommend liquidating the book.
    /// Fail-closed here means selecting NOTHING, and it must be an `Err` rather
    /// than an empty answer so the engine reports a reason.
    #[test]
    fn a_corrupt_or_missing_cap_refuses_rather_than_liquidating_at_the_ceiling() {
        let m = marks(&pos("longdated", 26, "2027-04-25", "12.6", "0.0312"));
        assert_eq!(select(&m, 16.0, CAP, &[], NOW).unwrap().0.len(), 1, "control");

        // `RiskView::load` on a missing/unparseable exec.yaml: the BANKROLL
        // parses to 0.0, and every cap in that file is a fraction of it, so the
        // cap is 0.0 and utilization fabricates 1.0.
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let e = select(&m, 16.0, bad, &[], NOW).expect_err("a cap of {bad} is not a cap");
            assert!(e.contains("global cap"), "and it names the input: {e}");
        }

        // ...and a hurdle that did not come off the utilization curve is
        // refused too, including the 0.0 a bench/no-risk run installs.
        for bad in [0.0, crate::APR_FLOOR - 0.1, crate::APR_CEIL + 0.1, f64::NAN] {
            assert!(select(&m, bad, CAP, &[], NOW).is_err(), "hurdle {bad} must refuse");
        }
    }

    /// WHAT THIS ENGINE COULD ACT ON IS NOT WHAT IT SELECTS.
    ///
    /// The armed process runs under `--rel-prefix xvus-nobel-peace-26
    /// --rel-prefix xvus-brazil-pres-26 --rel-prefix xvus-fedcut-26`, and every
    /// candidate the live marks yield is `xvus-france-pres-27` or
    /// `xvus-time-poty-26` — families it holds no quoter and no book
    /// subscription for. The candidate is still REPORTED, because the finding
    /// is that the two sets are disjoint; it is reported as unactionable so the
    /// report cannot be mistaken for a work queue.
    #[test]
    fn a_candidate_outside_the_processs_rel_prefix_scope_is_reported_unactionable() {
        let m = marks(&format!(
            "{},{}",
            pos("xvus-france-pres-27-mel", 26, "2027-04-25", "12.6", "0.0312"),
            pos("xvus-nobel-peace-26-x", 26, "2026-10-09", "12.6", "0.0312")
        ));
        let owned = ["xvus-nobel-peace-26".to_string(), "xvus-brazil-pres-26".to_string()];
        let (exits, _) = select(&m, 16.0, CAP, &owned, NOW).unwrap();
        assert_eq!(exits.len(), 2, "both are candidates: {exits:?}");
        let mine: Vec<&str> =
            exits.iter().filter(|e| e.actionable).map(|e| e.rel_id.as_str()).collect();
        assert_eq!(mine, ["xvus-nobel-peace-26-x"], "only the owned family is actionable");

        // no scope at all = the process loaded the whole registry (main.rs:387)
        let (all, _) = select(&m, 16.0, CAP, &[], NOW).unwrap();
        assert!(all.iter().all(|e| e.actionable), "an unscoped process owns everything");
    }

    /// A MARKET THAT HAS RESOLVED IS NEVER QUOTED INTO.
    ///
    /// Three independent ways, because the ledger carries baskets whose markets
    /// are gone (task #50: settled sports positions still `status: open`) and an
    /// exit ask into a dead book is an order that can only sit there.
    #[test]
    fn a_settled_or_settling_market_is_never_a_candidate() {
        let today = today_iso(NOW);

        // 1. past its resolve date, however good the numbers look
        let past = marks(&pos("done", 10, "2026-07-01", "1.0", "0.30"));
        let (e, s) = sel(&past, 16.0).unwrap();
        assert!(e.is_empty(), "{e:?}");
        assert!(matches!(s.as_slice(), [Skip::Settled { .. }]), "{s:?}");

        // ...including one resolving TODAY, which is settling right now.
        let now_res = marks(&pos("today", 10, &today, "1.0", "0.30"));
        assert!(sel(&now_res, 16.0).unwrap().0.is_empty());

        // 2. no live book, so no forward APR and no exit price. This is what a
        //    resolved market looks like to `mark_positions.py`.
        let dead = format!(
            r#"{{"generated_at":"{}","positions":[{{"relationship_id":"gone","ts":1.0,
                 "kalshi_ticker":"KX-gone","qty":10,"resolves_by":"2027-04-25",
                 "forward_hold_apr":null,"maker_exit_ct":null}}]}}"#,
            iso_z(NOW)
        );
        let (e, s) = sel(&dead, 16.0).unwrap();
        assert!(e.is_empty(), "{e:?}");
        assert!(matches!(s.as_slice(), [Skip::NotPriceable]), "{s:?}");

        // 3. a family whose exits this engine does not own at all.
        let sport = marks(&pos("sports-wta-A@B", 2, "2027-04-25", "1.0", "0.30"));
        let (e, s) = sel(&sport, 16.0).unwrap();
        assert!(e.is_empty(), "{e:?}");
        assert!(matches!(s.as_slice(), [Skip::HeldToSettlement]), "{s:?}");
    }

    /// MARKS THAT CANNOT BE TRUSTED DECIDE NOTHING — not "nothing to exit".
    ///
    /// Same rule and same code as the take-take bar (`taketake::bar_from_marks`),
    /// because it is the same file. The numbers are the 2026-07-28 incident's:
    /// marks stopped being written and the engine traded against the frozen file
    /// for four hours.
    ///
    /// The two files are read SEPARATELY, one tick apart, so they can briefly
    /// be two adjacent versions of it — but neither can act on a stale one,
    /// which is the property that matters.
    #[test]
    fn stale_or_corrupt_marks_refuse_rather_than_selecting_nothing() {
        let m = marks(&pos("longdated", 26, "2027-04-25", "12.6", "0.0312"));
        assert_eq!(sel(&m, 16.0).unwrap().0.len(), 1, "fresh: the candidate is there");

        assert!(select(&m, 16.0, CAP, &[], NOW + 15210.0).is_err(), "15210s behind must refuse");
        assert!(select(&m, 16.0, CAP, &[], NOW - 15210.0).is_err(), "a clock skewed forward too");
        assert!(sel(r#"{"generated_at":"2026-07-2"#, 16.0).is_err(), "torn write");
        assert!(sel(r#"{"positions":[]}"#, 16.0).is_err(), "unageable");

        // ...but an ABSENT file is a cold start, not a fault.
        assert_eq!(sel("", 16.0).unwrap().0.len(), 0);
    }

    /// WHAT THE FEEDBACK LOOP ACTUALLY GUARANTEES, which is less than the first
    /// draft of this file claimed.
    ///
    /// This proves ONE thing: `select` is a threshold on a per-basket value the
    /// hurdle does not touch, so lowering the hurdle always yields a SUBSET.
    /// It does NOT prove the loop is a ratchet. It cannot: an exit fill cannot
    /// lower `utilization()` anywhere in this tree — `record_open` is `+=` only
    /// and there is no `record_close` — so no test here can exhibit one. See §2
    /// of the module header for why a placer must debounce instead.
    #[test]
    fn a_lower_hurdle_selects_a_subset_because_the_rule_is_a_threshold() {
        let hi_bar = crate::apr_bar(1.0);
        let lo_bar = crate::apr_bar(0.933);
        assert!(lo_bar < hi_bar, "apr_bar is monotone in utilization: {lo_bar} vs {hi_bar}");

        let m = marks(&format!(
            "{},{}",
            pos("longdated", 26, "2027-04-25", "12.6", "0.0312"),
            pos("neardated", 4, "2026-12-09", "15.6", "0.0250")
        ));
        let hi = sel(&m, hi_bar).unwrap().0;
        let lo = sel(&m, lo_bar).unwrap().0;
        assert_eq!(hi.len(), 2, "both clear a 16%/yr hurdle: {hi:?}");
        assert_eq!(lo.len(), 1, "only the deeper one clears 15.2%/yr: {lo:?}");
        assert!(
            lo.iter().all(|e| hi.contains(e)),
            "the lower hurdle's set must be a SUBSET, never a new basket: {lo:?} vs {hi:?}"
        );

        // ...and that is the general property, not one lucky pair.
        for step in 0..=20 {
            let h = lo_bar + (hi_bar - lo_bar) * f64::from(step) / 20.0;
            let mid = sel(&m, h).unwrap().0;
            assert!(mid.iter().all(|e| hi.contains(e)), "at hurdle {h}: {mid:?}");
        }
    }

    /// THE SET IS THE SET, WHATEVER ORDER IT CAME OUT IN.
    ///
    /// `select`'s display order sorts by `(qty desc, fwd_apr asc)` and the live
    /// book routinely ties on BOTH keys — four identical 10-lot
    /// `brunoretailleau` baskets at the same rounded forward APR. Anything that
    /// edge-triggers on the display vector would re-print on a reorder that
    /// changed no decision, so the identity used for that is sorted.
    #[test]
    fn the_candidate_identity_is_a_set_not_the_display_order() {
        let a = pos("bret", 10, "2027-04-25", "11.1", "0.0312");
        let b = a.replace("1784646659.716", "1784727137.511");
        let ab = sel(&marks(&format!("{a},{b}")), 16.0).unwrap().0;
        let ba = sel(&marks(&format!("{b},{a}")), 16.0).unwrap().0;
        assert_eq!(ab.len(), 2);
        assert_ne!(ab, ba, "the display vector DOES flip on a tie — that is the hazard");
        assert_eq!(identity_set(&ab), identity_set(&ba), "the identity must not");
    }

    /// THE NEAR-MISS IS THE TELEMETRY. Four of the five skip causes render as
    /// `unwind_candidates: 0` and are not the same finding at all; the one that
    /// matters is the basket that cleared the APR test and failed only the exit
    /// floor, and what forcing that population out would cost.
    #[test]
    fn the_skips_are_tallied_so_nothing_selected_is_not_one_number() {
        let m = marks(&format!(
            "{},{},{},{}",
            pos("loser", 30, "2027-04-25", "13.8", "-0.0497"),
            pos("loser2", 10, "2027-04-25", "13.8", "-0.0301"),
            pos("holding", 5, "2026-12-09", "22.2", "0.30"),
            pos("sports-wta-A@B", 2, "2027-04-25", "1.0", "0.30")
        ));
        let (exits, skips) = sel(&m, 16.0).unwrap();
        assert!(exits.is_empty(), "{exits:?}");
        let t = SkipTally::of(&skips);
        assert_eq!(t.exit_unprofitable, 2);
        assert_eq!(t.hold_is_better, 1);
        assert_eq!(t.held_to_settlement, 1);
        // 30 x -0.0497 + 10 x -0.0301 = -1.792
        assert!((t.exit_unprofitable_usd - -1.792).abs() < 1e-9, "{}", t.exit_unprofitable_usd);
        assert!(t.describe().contains("exit-unprofitable 2"), "{}", t.describe());
    }

    /// THE CURATED RESOLVE DATE IS A GUESS, AND THE CANDIDATE SAYS SO.
    ///
    /// `resolve_dates.py` flags every long-dated family `estimated=True`
    /// ("event date not officially fixed"), and both halves of this decision
    /// rest on that date: `forward_hold_apr` is annualised to it and
    /// [`Skip::Settled`] compares against it. Refusing on the flag would select
    /// nothing ever, so it is carried.
    #[test]
    fn a_candidate_on_a_guessed_resolve_date_carries_the_flag() {
        let est = marks(&pos("longdated", 26, "2027-04-25", "12.6", "0.0312"));
        assert!(sel(&est, 16.0).unwrap().0[0].resolves_estimated, "the live shape is estimated");

        let firm = est.replace(r#""resolves_estimated":true"#, r#""resolves_estimated":false"#);
        assert!(!sel(&firm, 16.0).unwrap().0[0].resolves_estimated);
    }

    /// THE HAND-OFF. A standard basket is long Kalshi YES, so ONE of its two
    /// passive exits rests an ASK on the Kalshi market — the side
    /// `Quoter::target` already returns `None` for when it is suppressed, so
    /// the entry quoter cancels out and stays out rather than fighting its own
    /// unwind for the queue.
    ///
    /// It is not the only one. `maker_exit::Shape::RestPmUs` rests a BID on the
    /// PM-US leg instead and hands off the same way on that venue's bid side;
    /// which shape wins is a property of the two spreads and is decided per
    /// exit. What this pins is the Kalshi-side hand-off, which is unchanged.
    ///
    /// AND IT IS NOT UNIQUE PER BASKET (§5): two baskets on one ticker yield
    /// one key, one venue-side ask, and a placer that must aggregate them.
    #[test]
    fn the_side_an_exit_owns_is_the_kalshi_ask_the_entry_quoter_yields() {
        let m = marks(&pos("longdated", 26, "2027-04-25", "12.6", "0.0312"));
        let (exits, _) = sel(&m, 16.0).unwrap();
        assert_eq!(exits[0].suppress_key(), ("KX-longdated".to_string(), BookSide::Ask));

        let a = pos("bret", 10, "2027-04-25", "11.1", "0.0312");
        let b = a.replace("1784646659.716", "1784727137.511");
        let both = sel(&marks(&format!("{a},{b}")), 16.0).unwrap().0;
        assert_eq!(
            both[0].suppress_key(),
            both[1].suppress_key(),
            "N baskets, ONE ask: the placer has to aggregate and split fills back"
        );
    }

    /// WHY AN EXIT MUST NOT RESERVE, as arithmetic rather than as an opinion.
    ///
    /// `risk.rs` reserves a resting quote's contracts against the caps, and an
    /// exit quote rests. But `RiskGate::check` "gates OPENING risk only" and the
    /// reservation is that gate's committing half: the position's contracts are
    /// ALREADY in `by_rel`/`by_class` from `record_open`, so charging the exit
    /// for them again counts the same money twice — in the direction that
    /// forbids the trade that would give the money back.
    ///
    /// Concretely: a book at or over its class budget refuses every quote with
    /// a `class cap:` reason. An exit routed through that gate is refused for
    /// being too large while being the only thing that could make it smaller —
    /// a deadlock, not a safeguard.
    ///
    /// This pins EXISTING `risk.rs` behaviour, so it is a characterisation test
    /// and not a proof of anything new. Its job is to make the reserve decision
    /// fail loudly if a future change quietly routes exits through this gate.
    #[test]
    fn routing_an_exit_through_the_opening_cap_would_deadlock_the_cap() {
        // A config written out rather than read from `config/exec.yaml` — a
        // test binary's cwd is the crate dir, so that path does NOT exist here,
        // and `RiskView::load` answers a missing file with a $0 bankroll that
        // refuses everything. The assertion below would then have passed for a
        // reason that has nothing to do with the cap. The numbers are arbitrary
        // round ones; only their ratio to the clip matters.
        let d = std::env::temp_dir().join(format!("arb-unwind-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let exec = d.join("exec.yaml");
        std::fs::write(&exec, "bankroll_usd: 1000\nper_class_cap: 0.35\n").unwrap();
        let v = crate::risk::RiskView::load(
            &exec.to_string_lossy(),
            "/nonexistent/topics.yaml",
            vec![("kalshi".into(), "1000".into()), ("polymarket_us".into(), "1000".into())],
            std::collections::HashMap::new(),
        );
        // ...and that config is also what makes the cap REAL, which is the
        // input `select` refuses without (§4). $1000 x GLOBAL_CAP 0.50; the
        // per-class budget the check below fills is a separate, tighter $350.
        assert_eq!(v.global_cap_usd(), 500.0);
        let rel = arb_core::scan::Rel {
            id: "r1".into(),
            rtype: arb_core::scan::RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                arb_core::scan::RelLeg {
                    venue: arb_core::model::Venue::Kalshi,
                    market_id: "K".into(),
                },
                arb_core::scan::RelLeg {
                    venue: arb_core::model::Venue::PolymarketUs,
                    market_id: "P".into(),
                },
            ],
        };
        // ...and the gate is live: on an EMPTY book the same clip passes, so
        // the refusal below is the cap and not a broken fixture.
        let empty =
            arb_core::quoter::RiskGate::check(&v, &rel, arb_core::model::Venue::Kalshi, 20, None);
        assert!(empty.allowed, "fixture is not gating for the wrong reason: {:?}", empty.reasons);

        // ...and now the class budget ($1000 x 0.35) is full, and then some, as
        // it is live.
        v.record_open("already-open", "cross-venue-equivalent", 351.0);
        let d =
            arb_core::quoter::RiskGate::check(&v, &rel, arb_core::model::Venue::Kalshi, 20, None);
        assert!(!d.allowed, "the opening cap refuses the exit that would empty it");
        assert!(d.reasons.iter().any(|r| r.contains("class cap")), "{:?}", d.reasons);

        // AND EXPOSURE ONLY EVER GOES UP (§2). There is no `record_close`, so
        // the deadlock above cannot be cleared by an exit filling — only by a
        // restart re-seeding from the ledger.
        let full = v.utilization();
        v.record_open("already-open", "cross-venue-equivalent", 10.0);
        assert!(v.utilization() >= full, "utilization is monotone non-decreasing in-process");
    }
}
