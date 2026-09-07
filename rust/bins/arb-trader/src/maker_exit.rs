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
//! RESTS and only trades if somebody comes to us. Those are different trades
//! with different signs and the only reason this module can exist is that they
//! are different.
//!
//! # WHICH LEG RESTS IS A CHOICE, AND IT USED TO BE MADE BY ACCIDENT
//!
//! **RETRACTED: "one resting Kalshi ask".** For the whole life of this module
//! the passive leg was the Kalshi one, always, and the PM-US leg was always
//! crossed. That was never argued for — it was the first shape written and it
//! became the only shape. It is wrong on this book more often than it is right.
//!
//! An exit assigns each of the two legs a role, so the space is 2x2. Gross of
//! fees the two single-maker cells differ by exactly
//! `kalshi_spread - pmus_spread`: you capture the spread on the venue you rest
//! at and you pay it on the venue you cross. Fees tilt it a further
//! `0.0075 * p * (1 - p)` toward PM-US, which charges makers NOTHING while
//! Kalshi charges them 0.0175. **So the rule is "rest where the spread is
//! wider", and on this book that is overwhelmingly PM-US** — median Kalshi
//! spread 1-3c against 2-9c on PM-US across the eleven pairs this engine holds.
//! Resting on Kalshi meant capturing 3c and paying 8c, on every contract, for
//! as long as this module has existed. [`Shape`] carries the measurement.
//!
//! # WHAT THIS DOES, EXACTLY
//!
//! One basket, one lot, one resting order, one cross on the other venue:
//!
//!   1. `unwind::select` picks candidates against the hurdle in force;
//!   2. [`Debounce`] holds each one for [`DEBOUNCE_S`] of CONTINUOUS selection
//!      across at least [`DEBOUNCE_SCANS`] scans;
//!   3. the basis comes from `data/exec/trades.jsonl` — `naked_act::lot_at` on
//!      BOTH legs of the ONE record `select` named, addressed by its
//!      `opened_ts`;
//!   4. BOTH shapes are priced off that one basis and the better lock wins
//!      ([`decide`]). [`exit_limit`] solves for the resting leg when it is the
//!      Kalshi ask; [`close_limit`] solves for it when it is the PM-US bid.
//!      They are the same solver run with the other leg pinned, which is why
//!      the two shapes cannot drift apart;
//!   5. the entry quoter is asked to yield BOTH sides the winner might need —
//!      the shape is not known until the books are priced — and given
//!      [`SUPPRESS_SETTLE_S`] to do it. [`decide`] then checks the one it
//!      actually picked;
//!   6. ONE post-only GTC order rests, for at most [`MAX_CLIP`] contracts, on
//!      whichever venue won;
//!   7. on a fill the OTHER leg is closed with an IOC re-priced against the
//!      book AT THAT MOMENT, and one `unwound` record naming that lot's `ts`
//!      goes in through `ledger::append_basket`.
//!
//! Everything below that says "the ask" or "the Kalshi leg" about the RESTING
//! order is describing [`Shape::RestKalshi`] specifically. The interlocks —
//! [`MAX_RESTING`], the stand-off, the naked-leg halt — are shape-independent
//! and unchanged: there is still at most ONE exit outstanding in this process,
//! of either shape.
//!
//! # HOW §5's FIVE PROBLEMS ARE ANSWERED — THREE BY REFUSING TO HAVE THEM
//!
//!   * **AGGREGATE N BASKETS BEHIND ONE ASK, AND SPLIT A PARTIAL FILL BACK
//!     ACROSS N `closes_ts`.** NOT DONE, AND NOT NEEDED, because the trade was
//!     narrowed until the problem stopped existing. `unwind::select` already
//!     answers "which lot" — it names the basket by `(rel_id, opened_ts)` — and
//!     the exit is sized to THAT LOT ALONE. One exit therefore names exactly one
//!     `closes_ts` and no split can arise. The cost is that a ticker carrying
//!     six lots takes six passes to empty rather than one; the benefit is that
//!     the hardest correctness requirement in that header is structurally
//!     absent rather than implemented.
//!
//!     THIS USED TO SAY the lot was `worst_lot`'s dearest, and that pricing
//!     against the dearest was "the only attribution-safe choice — contracts are
//!     fungible, and an exit that pays against the dearest pays against every
//!     other one". The first half was true of the code and the second half is
//!     true of a ONE-LEGGED question, which this is not. `worst_lot` called once
//!     per leg pairs the dearest Kalshi record with the dearest PM-US record,
//!     and on a ticker with several lots those are DIFFERENT records: the
//!     composite is dearer than any basket we ever traded and can exceed the $1
//!     the pair pays, at which point no legal ask exits it. That is not
//!     conservative, it is unsatisfiable, and it is what held
//!     `maker_exit_closed` at 0. See `naked_act::lot_at`.
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
//!   * **§1, THE BLOCKER, IS RETRACTED IN FULL.** Both halves, on two dates,
//!     re-derived rather than inherited:
//!       - the half that BLOCKED went on 2026-07-31. That header was written
//!         when the armed process ran three families; it now runs
//!         `--rel-prefix xvus-`, and all 25 priced positions in
//!         `data/exec/marks.json` are `xvus-`. The candidate set and the owned
//!         set are no longer disjoint — they are identical. [`Live::target`]
//!         still refuses anything `unwind` marked unactionable, so the test is
//!         enforced rather than assumed.
//!       - the COVERAGE GAP went on 2026-08-14, and this is what unblocks the
//!         flywheel: an exit could only ever recycle capital somebody else's
//!         writer had booked. `marks::build` now derives a missing cost basis
//!         from the record's own legs (`marks::derived_basis`), so the
//!         `source: arb-trader` baskets that were absent from `marks.json` are
//!         rows in it — 31 records, about $133, on the ledger the day it
//!         landed. **THIS MODULE CAN NOW EXIT A BASKET THIS ENGINE OPENED**,
//!         and the previous sentence here said the opposite.
//!         Two things bound what that opened. The basis those rows carry is
//!         per-RECORD arithmetic, and `decide` re-derives it off the same record
//!         through `naked_act::lot_at`, so a row this module accepts and a lot
//!         it prices cannot disagree. (They could and did while `decide` used
//!         `worst_lot`: marks priced the selected record and `decide` priced the
//!         dearest leg of each venue, 0.9263 against 1.0023 on
//!         `xvus-time-poty-26-artificialintelligence`.) And an INVERTED engine
//!         basket
//!         (Kalshi `side:"ask"`) publishes `maker_exit_ct: null`, which
//!         `unwind::consider` refuses as `NotPriceable` before it is ever a
//!         candidate; `lot_at` would refuse its direction here in any case.
//!
//! # THE DEBOUNCE IS SIZED ON THE TAPE, NOT ON A GUESS
//!
//! See [`DEBOUNCE_S`]. The short version: on 4,735 samples of
//! `data/exec/marks_history.jsonl` spanning 172.7 h, 60% of every excursion
//! above the two-tick floor is ONE SAMPLE LONG.
//!
//! # THREE ORDER-OWNERS, ONE ACCOUNT, AND THE RULE THAT DECIDES COLLISIONS
//!
//! This is not the only thing sending Kalshi orders on these markets. The engine
//! hedges, `--positions-recon-act` completes naked legs from venue truth, and
//! this rests exits — and every Kalshi order this binary sends carries
//! `self_trade_prevention_type: taker_at_cross` (`arb_venue::wire`), which
//! answers a collision by CANCELLING THE TAKER. This module is the maker in
//! every one of those collisions, so its ask is never the order that dies. The
//! other one is, and it dies quietly: an IOC that crossed our own resting ask
//! comes back unfilled, and its owner reports that the book moved.
//!
//!   * **THE BACKSTOP COMPLETING A LEG WE ARE ALREADY WORKING.**
//!     [`working_check`] stands `naked_act` off every market in
//!     [`Live::working_set`] while an ARMED exit is working it. Armed only: a
//!     shadow exit rests nothing, so standing the backstop off would cost a real
//!     naked leg its completion for a trade that is not going to happen.
//!   * **OUR ASK ON A MARKET THE ENGINE OWES A HEDGE ON.** There the engine's
//!     hedge is the IOC, so the hedge is what `taker_at_cross` cancels and the
//!     leg it was covering stays naked. [`decide`] refuses through
//!     `naked_act::inflight_check` — the same registry the backstop reads, for
//!     the same reason.
//!   * **A NAKED LEG WE MADE OURSELVES.** [`Live::target`] refuses every new
//!     exit once [`UNRESOLVED`] is non-zero, and that counter never decrements,
//!     so the refusal is a LATCH cleared by a restart and by nothing else. What
//!     it prevents is the compounding shape: a close that failed leaves the
//!     ledger still calling the lot open, `unwind` re-selects it on the next
//!     scan, and a second ask rests against contracts the venue sold an hour
//!     ago. The refusal prints on every 60 s cycle while it holds, the same
//!     cadence as every other "nothing to rest:" line — that is the alarm
//!     working, not a loop to fix.
//!
//! THE STAND-OFF RELEASES ITSELF and nothing hands it over: an exit that fails
//! its close clears [`Live::resting`] and latches `target` off, so the very next
//! cycle publishes a working set without that market and the backstop is free to
//! complete the leg this module just left naked. The one exception is an order
//! this process could not address — a place or a cancel that never completed —
//! which joins [`Live::unaddressable`] and is never released, because nothing
//! here can learn that it is gone.
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
//! fight, and it is a reason to disarm one of them, not a curiosity. The
//! stand-off above narrows that fight to its one legitimate case — the backstop
//! completing a leg this module has ALREADY abandoned — and does not remove it.
//!
//! It also works ONE candidate per cycle and gives that cycle up when the
//! candidate refuses. [`Live::target`] returns the first admitted candidate of
//! `unwind::select`'s display order (`qty` descending, then forward APR) and
//! [`cycle`] pushes the refusal and returns rather than trying the next one, so
//! one candidate that refuses PERSISTENTLY — a floor above the bid, a PM-US book
//! gone dark, no `polymarket_us` leg in the registry — blocks every candidate
//! behind it for as long as it is selected. The debounce keeps folding for all
//! of them, so nothing is lost but time; what is lost is a paying exit waiting
//! behind an unexitable lot. It is left that way deliberately: trying the next
//! candidate means publishing the UNION of their suppression requests, because
//! `Engine::maker_exit_tick` drops the install stamp of any market absent from
//! the current request and a rotating request would therefore never accumulate
//! [`SUPPRESS_SETTLE_S`] — so the cheap-looking fix yields the entry quoter's
//! ask on three markets in order to rest at most one exit.
//!
//! It also prices the PM close off a book that INCLUDES OUR OWN RESTING SIZE.
//! `unwind::MIN_EXIT_CT` names this bias exactly — always adverse, a tick is a
//! lower bound on it and not a bound — and the answer here is the same
//! instalment: [`close_limit`] pays one full [`TICK`] THROUGH the ask it can
//! see. On a book where our own quote is the only ask that is not enough, and
//! nothing in this process can tell us when that is true.

use crate::ledger;
use crate::naked_act::{ceil_to_tick, lot_at, Held};
pub use crate::naked_act::MIN_LOCK;
use arb_core::fees::{FeeSchedule, Role};
use arb_core::clock::now_s as wall_now;
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use arb_venue::gateway::Quote;
use arb_venue::resp::OrderFill;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrd};
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
///
/// # RE-DERIVED 2026-08-25: 900 s -> 120 s, BECAUSE THE WRITER IT WAS SIZED
/// # AGAINST NO LONGER EXISTS
///
/// Every number above was measured on `marks_history.jsonl`, written by
/// `arbbot-marks.timer` at a MEDIAN PERIOD OF 120.4 s. That timer was retired on
/// 2026-07-31 and marks moved in-process: `data/exec/marks.json` is now rewritten
/// **about once a second** (sampled live on 2026-08-25 — a fresh `generated_at`
/// on every 400 ms poll).
///
/// THAT IS THE WHOLE OF WHY 900 WAS THE NUMBER, and it was never about the
/// market. [`Debounce::admit`] requires BOTH a wall-clock age and
/// [`DEBOUNCE_SCANS`] observations, and it is called once per [`CYCLE_S`] — 60 s.
/// Against a 120 s writer, three 60 s scans could read THE SAME FILE three times
/// and count as three samples while being one; the wall-clock term existed to
/// force them across enough writer periods to be independent. Against a 1 s
/// writer every scan is already an independent sample, so the term is doing
/// nothing the scan count does not already do.
///
/// THE MINIMUM THAT PRESERVES THE PROTECTION EXACTLY:
///
/// ```text
///   (DEBOUNCE_SCANS - 1) * CYCLE_S  =  2 * 60  =  120 s
/// ```
///
/// which is the time three independent scans take anyway, so the two conditions
/// now bind together and neither is slack. Below this the wall-clock term stops
/// binding at all and the scan count alone decides; above it we are waiting for
/// no measured reason. It is also, exactly, ONE 120-SECOND WINDOW — the unit the
/// failure mode is stated in: a value that existed for one sample cannot survive
/// being asked for again one whole window later.
///
/// WHAT THIS COSTS, STATED PLAINLY. The 27%/33%/55% survival figures were
/// durations measured in 120 s samples, so a 120 s bar admits excursions a 900 s
/// bar refused — more candidates, and some of them will die before an ask on
/// them fills. Two things carry that which did not exist when 900 was chosen:
/// [`still_pays`] pulls a resting exit the book has moved away from, and a CROSS
/// ([`Order::cross`]) does not care at all, because it fills or dies in the same
/// second and has nothing left to be wrong about.
///
/// **THE 60% FIGURE CANNOT BE RE-MEASURED AT THE NEW RESOLUTION**, and this does
/// not pretend otherwise: `marks_history.jsonl` stopped on 2026-07-31, so there
/// is no 1 s-resolution history to re-derive an excursion distribution from.
/// What is re-derived here is the SIZING RULE, which was a property of the
/// writer and the cycle, not of the book.
pub const DEBOUNCE_S: f64 = (DEBOUNCE_SCANS as f64 - 1.0) * CYCLE_S as f64;

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

/// The profit floor a maker exit must clear to be OPENED or KEPT, per contract,
/// net of both legs' fees and against both legs' ledger basis.
///
/// **THE SAME NUMBER THE SELECTOR ADMITTED THE LOT UNDER.** `unwind::consider`
/// refuses anything under `MIN_EXIT_CT` (0.02) as not worth doing, and this is
/// the decision that acts on what it admitted, so the two must be one floor.
///
/// IT USED TO BE `MIN_LOCK` (0.005), and that was a borrowed constant doing the
/// wrong job: it is ported verbatim from `hedge_naked_legs.py` and was chosen
/// for a leg that is ALREADY NAKED and needs out — a case where half a cent
/// beats carrying direction for months. Nothing about it was ever an answer to
/// "should we exit a hedged position at all".
///
/// The gap was reachable rather than theoretical. `consider` screens on the
/// MARKS snapshot, which `select` will price off at up to 900 s old, while
/// `decide` re-prices against the book NOW — so a lot admitted at 2c could go
/// out at 0.6c on a book that had moved. That is precisely the case of owning
/// at 0.95 and selling at 0.96: refused by the screen, allowed by the floor.
///
/// **NOT USED BY `price_close`, DELIBERATELY.** That prices the SECOND leg,
/// after the first has already filled, and a refusal there does not decline a
/// trade — it strands a naked leg. `risk.rs` states the governing invariant for
/// the symmetric case: "never consult this for a HEDGE. Refusing a hedge leaves
/// the first leg naked, which is strictly worse than being a little over
/// budget." Completing a close is the same trade with the same asymmetry, so it
/// keeps `MIN_LOCK` and `heal` keeps its cross-out.
const EXIT_FLOOR: &str = crate::unwind::MIN_EXIT_CT_S;

/// Exits resting at once, across the whole process. ONE.
///
/// This is the answer to `unwind` §5's "a naive placer rests a SECOND ask", and
/// it is a cap rather than a per-market interlock on purpose: a per-market rule
/// would still let one pass rest five asks on five markets, five separate
/// one-legged exposures, each of which becomes a naked PM-US short the moment it
/// fills. One at a time means at most one leg can be in flight, ever.
pub const MAX_RESTING: usize = 1;

/// How long a resting exit may hold the single [`MAX_RESTING`] slot while other
/// candidates are held behind it. One hour.
///
/// THIS IS AN OPPORTUNITY-COST BOUND, NOT A PRICE CHECK. [`still_pays`] already
/// pulls an ask the PM book has moved away from; nothing pulled one that was
/// simply never going to fill. On 2026-08-14 a 4-lot ask on `KXTIME-26-AI` rested
/// for 2.3 days without filling, and because [`MAX_RESTING`] is 1 it held the
/// whole recycler shut against as many as 9 held candidates the entire time. The
/// single slot is the scarce resource and nothing was rationing it.
///
/// ONLY WHEN THE SLOT IS CONTESTED, which is the whole of the rule. Rotating an
/// ask costs its queue position at the venue, and paying that to re-rest the same
/// exit at the same price is a pure loss. So age alone does not pull:
/// [`Live::target`] arms this only while at least one held candidate sits on a
/// DIFFERENT market, i.e. only when something else would actually use the slot.
///
/// An hour is 60 cycles and 4x [`DEBOUNCE_S`]. Long enough that a passive fill
/// has had a fair run at the queue; short enough that one dead ask costs the
/// recycler an hour rather than a weekend. A candidate still worth exiting is
/// still admitted by the debounce when it comes back round, so a rotation
/// re-prices it against a fresh book rather than abandoning it.
pub const MAX_RESTING_S: f64 = 3600.0;

/// Cycles [`heal`] will retry a failed close PROFITABLY-ONLY before it crosses
/// out regardless of price. Ten, so ten minutes.
///
/// WHY IT CROSSES AT ALL. Being one-legged after an exit is not a pricing
/// problem, it is a directional position: the Kalshi contracts are sold and the
/// PM-US side is naked until the market resolves, which for this book is 51 to
/// 249 days away. `risk.rs` already states the governing invariant for the
/// symmetric case — "never consult this for a HEDGE. Refusing a hedge leaves the
/// first leg naked, which is strictly worse than being a little over budget" —
/// and completing a close is the same trade with the same asymmetry. The sizes
/// make it lopsided rather than arguable: [`MAX_CLIP`] is 5 and [`MAX_RESTING`]
/// is 1, so at most five contracts are ever naked at once. Crossing out costs
/// cents; carrying them costs up to $5 of unhedged direction for months.
///
/// WHY THERE IS A PROFITABLE-ONLY WINDOW FIRST. Most of the ways a close fails
/// are transient — an IOC that missed, a book that moved for a second, a refused
/// order — and on the next cycle the same close often pays. Ten cycles is the
/// cheap half of the trade; the eleventh admits it is not going to be cheap and
/// takes the flat position anyway.
///
/// THE PROFITABLE-ONLY HALF CANNOT BE THE WHOLE RULE, which is the measurement
/// that settled this. `--positions-recon-act` is the backstop the old alarm text
/// pointed at, and it is profitable-only: `positions_recon_acted` is 0 across
/// thousands of refusals over the life of the deployment, because a leg that is
/// underwater fails its profit floor every time. A profitable-only self-heal
/// inherits that 0% rate in exactly the case that needs it.
const HEAL_PROFITABLE_CYCLES: u32 = 10;

/// How old a resting exit must be before a 404 on its id is read as GONE rather
/// than as NOT YET VISIBLE.
///
/// Neither venue's create is read-your-writes (`gateway::Settle`): Kalshi 404s a
/// GET on an order it has just accepted, which is why `filled_qty` already polls
/// through `Settle::retry_404` — 8 attempts 500 ms apart, about 4 s. A 404 that
/// survives that window on an order placed seconds ago may still be the query
/// service lagging. A 404 on an order that has been resting a whole cycle cannot
/// be: the venue has answered about this id many times since.
///
/// One cycle ([`CYCLE_S`]) is 15x the settle window, and is the coarsest clock
/// this module has anyway — `manage` runs once a cycle, so the first read that
/// could observe a 404 at this age is already a cycle old.
const VANISHED_MIN_AGE_S: f64 = CYCLE_S as f64;

/// How long the entry quoter is given to yield the Kalshi ask before an exit is
/// rested on it.
///
/// 30 s: the suppress set is installed on the 60 s `apr_tick`, so this is one
/// tick plus a cancel round trip. See the module header for why a bounded wait
/// rather than a proof is tolerable HERE and was not tolerable for the Python's
/// IOC.
/// How many SELECTIONS a rotation's hand-over survives before the incumbent may
/// take the slot back.
///
/// **IT USED TO BE ONE, AND ONE CAN NEVER SUCCEED.** [`Live::target`] consumed
/// `rotated_out` on the selection that read it — "a one-selection courtesy" —
/// but the cycle that first selects a market STRUCTURALLY CANNOT PLACE on it:
/// [`decide`] refuses everything whose entry quote has not been yielded yet,
/// and the yield is only requested by that same cycle. So the courtesy was
/// always spent by a guaranteed refusal, and the incumbent — which `select`
/// very often still orders first — reclaimed the slot on the very next cycle.
///
/// Seen end to end on 2026-08-23, with `KXNOBELPEACE-26-DJT` holding the only
/// slot against 20 other eligible rows worth about $144:
///
/// ```text
///   05:00:49  ROTATING - DJT pulled so one of 7 waiting can have it
///   05:01:49  NO - xvus-btcmax-...-150k: quoter not yet told to yield  <- spent
///   05:02:49  NO - xvus-nobel-peace-26-donaldtrump: ...                <- reclaimed
///   05:03:50  RESTED 5x tac-nobel-peace-...-dontru                     <- incumbent
/// ```
///
/// THREE, because the handshake costs two — one cycle to ask, one to place,
/// since [`CYCLE_S`] is 60 and [`SUPPRESS_SETTLE_S`] is 30 — and one spare buys
/// the challenger a single bad cycle (a halted market, an unreadable quote)
/// without handing the slot back.
///
/// BOUNDED, and that is the other half. A challenger that can never be priced
/// would otherwise lock the incumbent out for ever, which is a worse starvation
/// than the one this fixes: an empty slot earns nothing at all, while the
/// incumbent's ask at least pays if somebody lifts it.
///
/// WHAT THE BUDGET BUYS BACK IS ONE NAME, not the lap. `Live::served` is a
/// queue of everyone who has already had the slot, and reaching this budget
/// pops its FRONT — the market excluded longest becomes eligible again while
/// the rest are still owed a turn. Clearing the whole lap here would restore
/// the two-name swap this exists to break, and it would do it precisely when
/// the book is hardest to price, which is when the queue matters most.
pub const HANDOVER_CYCLES: u32 = 3;

pub const SUPPRESS_SETTLE_S: f64 = 30.0;

/// The price increment both legs quote in, for the tick THROUGH the PM ask.
/// `mark_positions.py` steps PM in `Decimal("0.01")`; this is the resolution of
/// the venue, not a modelling choice.
const TICK: &str = "0.01";

/// The wire spellings of the two book sides, which is how [`EngineView::
/// suppressed_at`] and the suppression request are keyed. See that field.
const SIDE_ASK: &str = "ask";
const SIDE_BID: &str = "bid";

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
/// Unresolved legs this process closed BY ITSELF, via [`heal`].
///
/// A SECOND COUNTER RATHER THAN A DECREMENT ON [`UNRESOLVED`].
///
/// NOT because decrementing would break the page. An earlier draft of this
/// comment claimed a heal-then-relapse would return the gauge to a value it had
/// already reported and so fail to read as a rise; that is FALSE, and driving
/// `scripts/gauge_deltas.py` through 0 -> 1 -> 0 -> 1 shows it paging both
/// times. Its RISE rule re-clamps with `base = min(base, cur)` for exactly this
/// case — "a level that fell needs its baseline to fall with it".
///
/// The real reasons are quieter and all three still hold:
///
///   * `UNRESOLVED` is an INCIDENT COUNT. Decrementing it answers "are we naked
///     now", which [`outstanding`] already answers, and destroys "how many times
///     has this happened" — the number that says whether the close path is
///     unreliable rather than unlucky.
///   * `gauge_deltas.py` picked RISE *because* this gauge is a ratchet, and says
///     so. Making it fall would leave that rule reading something it was not
///     chosen for.
///   * `maker_exit_healed` is worth reading on its own: it is how an operator
///     tells "the engine fixed it" from "nothing has happened yet", which a
///     single gauge returning to 0 cannot express.
static HEALED: AtomicU64 = AtomicU64::new(0);

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
pub fn healed() -> u64 {
    HEALED.load(AtomicOrd::Relaxed)
}

/// Naked legs this process is carrying RIGHT NOW: raised minus healed.
///
/// This is the latch. It replaces the old `unresolved() > 0` test, which could
/// only ever be cleared by restarting the process — correct while nothing could
/// close a naked leg, and the wrong shape once something can.
pub fn outstanding() -> u64 {
    unresolved().saturating_sub(healed())
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
    /// PM-US market -> best YES BID, the price a RESTING close bids against.
    /// [`Shape::RestPmUs`] needs this the way [`Shape::RestKalshi`] needs
    /// `pm_ask`: the leg it prices passively is the one it must see both sides
    /// of, since post-only forbids resting at or through the offer.
    pub pm_bid: BTreeMap<String, String>,
    /// Kalshi market -> best YES BID, the price [`Shape::RestPmUs`] sells into
    /// at fill time. `Shape::RestKalshi` never reads it: that shape gets its
    /// Kalshi prices from a fresh `market_quote`, because it must also know the
    /// ladder and the trading status before it may rest there.
    pub k_bid: BTreeMap<String, String>,
    /// (market, side) pairs the quoters have been told to yield, and the instant
    /// that was installed. Absent means NOT yielded. The side is the WIRE
    /// spelling and not [`arb_core::model::BookSide`], which is deliberately not
    /// `Ord` (`quoter::cancel_all` sorts by the wire spelling, where
    /// `"ask" < "bid"`, and a derived `Ord` would reverse it — there is a test
    /// guarding exactly that). Keying on the spelling keeps this map ordered the
    /// way the rest of the codebase orders sides, and leaves that guard alone.
    ///
    /// THE SIDE IS PART OF THE KEY and was not always. While the only shape
    /// rested a Kalshi ask, "the market" and "the side to yield" were the same
    /// fact and the engine hardcoded `BookSide::Ask`. [`Shape::RestPmUs`] rests
    /// a PM-US BID, and yielding that market's ask would suppress the wrong
    /// quote while ours collided with the right one.
    pub suppressed_at: BTreeMap<(String, String), Instant>,
}

struct Published {
    view: EngineView,
    at: Instant,
}

static VIEW: Mutex<Option<Published>> = Mutex::new(None);
static SUPPRESS_REQ: Mutex<Option<BTreeSet<(String, String)>>> = Mutex::new(None);

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

/// Ask the entry quoters to yield these (market, side) pairs. Read by
/// `Engine::maker_exit_tick`.
pub fn request_suppress(markets: BTreeSet<(String, String)>) {
    if let Ok(mut g) = SUPPRESS_REQ.lock() {
        *g = Some(markets);
    }
}

/// The outstanding suppression request, for the engine.
pub fn suppress_requests() -> BTreeSet<(String, String)> {
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

// -------------------------------------------------------------- the stand-off ---

/// The Kalshi markets this module is WORKING: an ask resting on one, an ask
/// about to be rested on one, or an order it can no longer address.
///
/// PUBLISHED WHOLESALE at the top of every cycle, for [`EngineView`]'s reason —
/// a missed incremental update here would be a backstop IOC crossing our own
/// resting ask and being cancelled for it — and for one more. The RELEASE of a
/// market is derived rather than coded: the set is rebuilt from [`Live`] on
/// every cycle, so an exit that fills, is pulled or halts stops appearing in it
/// without anything having to remember to hand the market back.
struct Working {
    markets: BTreeSet<String>,
    at: Instant,
}

static WORKING: Mutex<Option<Working>> = Mutex::new(None);

/// Whether anybody is stood off at all. Set by [`arm_standoff`], never cleared.
static STANDOFF: AtomicBool = AtomicBool::new(false);

/// How stale a publication may be and still stand a backstop off. Three cycles
/// of the publisher, [`VIEW_MAX_AGE`]'s reasoning and its arithmetic: a loop
/// that has stopped publishing is one nothing can ask about, and the honest
/// answer to "is a post-only ask of ours resting there" from a silent loop is
/// not "no".
const WORKING_MAX_AGE: Duration = Duration::from_secs(CYCLE_S * 3);

/// Stand the naked-leg backstop off the markets this module is working. Called
/// from `main::spawn_maker_exit`'s ARMED branch and nowhere else.
///
/// NEVER from the shadow branch, and that asymmetry is the whole of it: a shadow
/// exit rests nothing, so there is no ask of ours for anything to collide with,
/// and standing the backstop off would cost a real naked leg its completion for
/// the sake of a trade that is not going to happen.
pub fn arm_standoff() {
    STANDOFF.store(true, AtomicOrd::Relaxed);
}

/// Publish what this module is working. Called from [`cycle`] and nowhere else.
pub fn publish_working(markets: BTreeSet<String>) {
    if let Ok(mut g) = WORKING.lock() {
        *g = Some(Working { markets, at: Instant::now() });
    }
}

/// May another order-owner in this process send a TAKER order on `market`?
///
/// `taker_at_cross` cancels the TAKER (`arb_venue::wire`), so an IOC that lifts
/// our own resting exit ask does not trade and does not error: it returns
/// unfilled and `positions.rs` reports that the book moved. The naked leg it was
/// sent to complete stays naked and nothing anywhere names the reason.
///
/// FAIL-OPEN on "no armed maker exit" and FAIL-CLOSED on everything else. That
/// is not `naked_act::inflight_check`'s asymmetry and the difference is
/// deliberate: with nothing armed there is no ask of ours to cross, so a refusal
/// would cost a naked leg its completion for nothing, while an armed exit whose
/// registry cannot answer is a registry saying "I cannot tell whether an `x` ask
/// of ours is resting there".
pub fn working_check(market: &str) -> Result<(), String> {
    if !STANDOFF.load(AtomicOrd::Relaxed) {
        return Ok(());
    }
    let g = WORKING.lock().map_err(|_| {
        "the maker-exit working registry is poisoned — it cannot say whether a post-only exit \
         ask of ours is resting on this market, and a taker that crosses one is CANCELLED by \
         taker_at_cross rather than filled"
            .to_string()
    })?;
    let Some(w) = g.as_ref() else {
        return Err(
            "the maker exit is ARMED and has not yet published what it is working — until it \
             does, a taker order here may cross an `x` ask of ours and be cancelled by \
             taker_at_cross while reporting only that the book moved. It publishes on its own \
             cycle, so this clears itself; if it does not, the exit loop is not running."
                .into(),
        );
    };
    let age = w.at.elapsed();
    if age > WORKING_MAX_AGE {
        return Err(format!(
            "the maker exit's working set is {:.0}s old (it publishes every {CYCLE_S}s) — the \
             exit loop is not running, so nothing here can tell whether an `x` ask of ours is \
             resting on {market}, and taker_at_cross would answer this order by cancelling it \
             rather than filling it",
            age.as_secs_f64()
        ));
    }
    if w.markets.contains(market) {
        return Err(format!(
            "a maker exit is working {market} — a post-only ask of ours is resting there or is \
             about to be, and `self_trade_prevention_type: taker_at_cross` answers a taker that \
             crosses it by CANCELLING THE TAKER, which is this order. It would come back \
             unfilled and be reported as the book having moved. The market is released on the \
             first cycle after the exit fills, is pulled, or halts on an unresolved leg"
        ));
    }
    Ok(())
}

/// [`WORKING`] and [`STANDOFF`] are process-wide, and the tests that touch them
/// span two modules: this one publishes and `naked_act::decide` reads. They all
/// serialise on `naked_act::TEST_SERIAL` — the guard the reader's own tests
/// already take — rather than on [`VIEW_TEST_SERIAL`], which would leave the two
/// halves free to interleave with each other.
#[cfg(test)]
pub(crate) fn reset_standoff() {
    STANDOFF.store(false, AtomicOrd::Relaxed);
    if let Ok(mut g) = WORKING.lock() {
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

// ---------------------------------------------------------------- the shape ---

/// WHICH LEG RESTS. An exit assigns each of the two legs a role — maker (rest
/// and wait) or taker (cross and be done) — so the space is 2x2, and this names
/// the two cells with exactly one resting leg.
///
/// # Why the other two cells are not here
///
///   * **take/take** is the LIQUIDATION, and it is already priced: it is
///     `marks`' `liq_value_usd`, `unwind_apr` annualises it, and on this book it
///     is a large realized loss (the france lots, about -111%/yr). It is not an
///     exit this module will ever place; the module header's first section is
///     about precisely that.
///   * **rest/rest** would capture BOTH spreads and is the most profitable cell
///     on paper. It is also the only one that is not an exit: it flattens only
///     if BOTH legs fill, and a one-sided fill is a naked directional position
///     that nothing cancels. That is a different risk class from anything here
///     — [`MAX_RESTING`] is 1 for the same reason — so it is priced for the
///     record by [`quote_shapes`] and never placed.
///
/// # Why RestPmUs exists at all, measured rather than assumed
///
/// Gross of fees the two single-maker shapes differ by exactly
/// `kalshi_spread - pmus_spread`: resting captures the spread on the venue you
/// rest at and crossing pays it on the venue you cross. Fees tilt it a further
/// 0.0075*p*(1-p) toward PM-US, because PM-US charges makers NOTHING
/// (`fees.rs`, `Role::Maker => cx.zero()`) while Kalshi charges makers 0.0175.
/// So the rule is "rest where the spread is wider", and on this book that is
/// overwhelmingly PM-US.
///
/// On the TOB tape for the eleven pairs this engine holds (`data/tob`,
/// time-matched, forward-filled, dearest lot per pair), median Kalshi spread is
/// 1-3c against 2-9c on PM-US, and [`Shape::RestPmUs`] wins on ten of eleven.
/// The two that matter most are the ones the module header calls unexitable:
///
/// ```text
///   KXFRENCHPRES-27-BRET   A eligible 29% of samples -> B 90%   +7.68c/ct
///   KXFRENCHPRES-27-FHOL   A eligible  0% of samples -> B 81%   +5.41c/ct
///   KXBTCMAXY-26DEC31-149999.99   A 4% -> B 59%                 +0.49c/ct
/// ```
///
/// FHOL is the case that makes this a correctness fix rather than an
/// optimisation: under the old single shape there is NO legal Kalshi ask that
/// exits it at a profit, at any price, ever — `exit_limit` refuses it — while a
/// PM-US bid at the touch exits it profitably four samples in five.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// Rest a post-only Kalshi ask (sell the YES we hold); on fill, cross the
    /// PM-US ask to buy the YES back. The original and only shape until now.
    RestKalshi,
    /// Rest a post-only PM-US bid (buy back the YES we are short); on fill,
    /// cross the Kalshi bid to sell the YES we hold.
    RestPmUs,
}

impl Shape {
    /// The fee role each leg pays under this shape. `(kalshi, pmus)`.
    pub fn roles(self) -> (Role, Role) {
        match self {
            Shape::RestKalshi => (Role::Maker, Role::Taker),
            Shape::RestPmUs => (Role::Taker, Role::Maker),
        }
    }

    /// The venue the post-only order rests at.
    pub fn rest_venue(self) -> Venue {
        match self {
            Shape::RestKalshi => Venue::Kalshi,
            Shape::RestPmUs => Venue::PolymarketUs,
        }
    }

    /// The venue the IOC closes at — the other one.
    pub fn close_venue(self) -> Venue {
        match self {
            Shape::RestKalshi => Venue::PolymarketUs,
            Shape::RestPmUs => Venue::Kalshi,
        }
    }

    /// Short tag for log lines and the ledger record.
    pub fn tag(self) -> &'static str {
        match self {
            Shape::RestKalshi => "rest-kalshi",
            Shape::RestPmUs => "rest-pmus",
        }
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
    k_role: Role,
    pm_role: Role,
    floor: &str,
) -> Result<D, String> {
    let lock = cx.parse_exact(floor);
    // What the PM leg gives back, net of the fee to take it.
    let size = cx.from_i64(qty);
    let pm_fee_total = fees.fee(cx, Venue::PolymarketUs, pm_role, pm_close, size, "");
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
        let k_fee_total = fees.fee(cx, Venue::Kalshi, k_role, l, size, "");
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

/// The price to actually REST at: the best offer this lot can legally make.
///
/// # Why the floor is not already the answer
///
/// [`exit_limit`] answers "what is the least this lot may be sold for", and for
/// the life of this module that number went to the venue unmodified. It is a
/// quote only when it happens to land above the bid. When it landed at or below
/// one, [`decide`] REFUSED — it read an ask under the bid as "a take dressed as
/// a maker order", which is true of the ORDER and false of the SITUATION. A
/// floor below the bid means the book is bidding MORE than this lot needs.
/// That is the best case an exit can be handed, and it was the one case the
/// module walked away from: 131 times on 2026-08-20, against a
/// `KXBTCMAXY-26DEC31-149999.99` floor of 0.0200 into a bid of 0.0300.
///
/// The invariant behind the refusal is kept, because it is a real one:
/// post-only rejects an ask at or below the bid, and an ask that crossed would
/// trade at a price nothing here decided to take. The answer to "our price is
/// too low to rest" is to RAISE it.
///
/// # Why the touch and not one rung over the bid
///
/// Because `marks` already said so, and the two have to agree. `maker_exit_ct`
/// — the number `unwind::consider` gates eligibility on — is computed at
/// `k_ask - TICK`, "one tick inside the competing ask". A placer that rested at
/// `bid + TICK` instead would be selecting exits on one price and asking
/// another: on a three-tick book like `KXBRPRES-26-FBOL` (bid 0.32, ask 0.35)
/// marks promises 0.34 and the wire would see 0.33. That is the same shape as
/// the bug this module was just fixed for — the marker and the placer pricing
/// different things — so it is answered the same way, by making them one rule.
///
/// UNDERCUTTING THE OFFER IS ALSO THE BETTER TRADE. `ask - TICK` is strictly in
/// front of every ask resting at the touch, so it buys the same queue position
/// `bid + TICK` does, and it collects the spread instead of giving it away.
/// Where the spread is one tick the two collapse onto the same rung anyway,
/// which is most of this book.
///
/// # The three floors, in order
///
///   * NEVER at or under the bid — post-only rejects it. This one is hard.
///   * never under [`exit_limit`]'s floor — that is the whole profit test, and
///     where the floor sits ABOVE the touch it wins and the ask rests outside
///     the market. That is the honest answer for a lot the book will not pay
///     for yet, and [`MAX_RESTING_S`] bounds what being wrong about it costs.
///   * never at or over $1, which is not a price this pair can pay.
fn rest_price(
    cx: &mut Cx,
    ladder: &[(String, String, String)],
    floor: D,
    yes_bid: Option<&str>,
    yes_ask: Option<&str>,
) -> Result<D, String> {
    // Neither side is not a book, and a missing side is not a side priced at
    // zero — `gateway::Quote` carries that distinction precisely so this does
    // not have to guess. With nothing to price against, the floor stands.
    let bid = yes_bid.and_then(|b| cx.parse(b));
    let ask = yes_ask.and_then(|a| cx.parse(a));
    // One rung INSIDE the competing ask: in front of the whole offer queue.
    let inside = match ask {
        Some(a) => {
            let (_, step) = rung_step(cx, ladder, a)?;
            let down = cx.sub(a, step);
            cx.is_pos(down).then_some(down)
        }
        None => None,
    };
    // ...but never at or under the bid, whatever the spread is. Where the
    // spread is one tick `inside` IS the bid, and this is what lifts it off.
    let over_bid = match bid {
        Some(b) => {
            let (_, step) = rung_step(cx, ladder, b)?;
            let up = cx.add(b, step);
            ceil_to_tick(cx, ladder, up)
        }
        None => None,
    };
    let touch = match (inside, over_bid) {
        (Some(i), Some(o)) => Some(if cx.cmp(i, o) == Ordering::Greater { i } else { o }),
        (x, None) => x,
        (None, y) => y,
    };
    let Some(touch) = touch else { return Ok(floor) };
    let p = if cx.cmp(floor, touch) == Ordering::Greater { floor } else { touch };
    let Some(p) = ceil_to_tick(cx, ladder, p) else {
        return Err(format!(
            "{} is outside every rung of the venue's tick ladder, so it has no legal spelling",
            cx.emit_6dp(p)
        ));
    };
    if let Some(b) = bid {
        if cx.cmp(p, b) != Ordering::Greater {
            return Err(format!(
                "the book is bid {} and the best post-only price the ladder offers is {} — \
                 not above it, so this exit has no price that is not a take",
                cx.emit_6dp(b),
                cx.emit_6dp(p)
            ));
        }
    }
    if cx.cmp(p, cx.one) != Ordering::Less {
        return Err(format!(
            "the best post-only price for this exit is {} — at or above $1, which is not a \
             price this pair can pay",
            cx.emit_6dp(p)
        ));
    }
    Ok(p)
}

/// The price to actually REST a PM-US BID at: the dearest this lot may bid and
/// still be a maker. The mirror of [`rest_price`], and every clause is that
/// function's clause with the inequality turned over.
///
/// We are SHORT the PM-US YES and closing it means BUYING the YES back. Buying
/// passively is a resting bid, so:
///
///   * one tick INSIDE the competing bid (`bid + TICK`) puts us in front of the
///     whole bid queue, exactly as `ask - TICK` fronts the offer queue;
///   * never AT OR ABOVE the ask — post-only rejects a bid that would cross,
///     which is [`rest_price`]'s hard floor pointing the other way. Where the
///     spread is one tick the cap collapses onto the bid and we JOIN it;
///   * never above `ceiling`, which is [`close_limit`]'s answer and the whole
///     profit test. Where the ceiling sits BELOW the touch the bid rests under
///     the market — the honest answer for a lot the book will not sell to us
///     cheaply enough yet, bounded by [`MAX_RESTING_S`] exactly as the ask is.
///
/// PM-US quotes one flat cent ladder (`quantize_down`'s note), so unlike the
/// Kalshi side there is no tapered rung to be relative to and TICK is the step
/// everywhere.
fn rest_price_bid(
    cx: &mut Cx,
    ceiling: D,
    yes_bid: Option<&str>,
    yes_ask: Option<&str>,
) -> Result<D, String> {
    let tick = cx.parse_exact(TICK);
    let bid = yes_bid.and_then(|b| cx.parse(b));
    let ask = yes_ask.and_then(|a| cx.parse(a));
    // One tick in front of the bid queue.
    let inside = bid.map(|b| cx.add(b, tick));
    // ...but strictly under the offer, whatever the spread is.
    let under_ask = ask.and_then(|a| {
        let d = cx.sub(a, tick);
        cx.is_pos(d).then_some(d)
    });
    let touch = match (inside, under_ask) {
        (Some(i), Some(u)) => Some(if cx.cmp(i, u) == Ordering::Less { i } else { u }),
        (x, None) => x,
        (None, y) => y,
    };
    // With no book at all the ceiling stands, exactly as the floor does.
    let Some(touch) = touch else { return Ok(ceiling) };
    let p = if cx.cmp(ceiling, touch) == Ordering::Less { ceiling } else { touch };
    let p = quantize_down(cx, p, tick);
    if !cx.is_pos(p) {
        return Err(format!(
            "the best post-only bid this lot may make is {} — not a price, so this exit has \
             no passive PM-US price",
            cx.emit_6dp(p)
        ));
    }
    if let Some(a) = ask {
        if cx.cmp(p, a) != Ordering::Less {
            return Err(format!(
                "the book is offered {} and the best post-only bid the ladder offers is {} — \
                 not below it, so this exit has no price that is not a take",
                cx.emit_6dp(a),
                cx.emit_6dp(p)
            ));
        }
    }
    Ok(p)
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
#[allow(clippy::too_many_arguments)]
pub fn close_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    k_fill: D,
    k_basis: D,
    pm_basis: D,
    qty: i64,
    k_role: Role,
    pm_role: Role,
    floor: &str,
) -> Result<D, String> {
    let lock = cx.parse_exact(floor);
    let size = cx.from_i64(qty);
    let k_fee_total = fees.fee(cx, Venue::Kalshi, k_role, k_fill, size, "");
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
        let fee_total = fees.fee(cx, Venue::PolymarketUs, pm_role, p, size, "");
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

/// CROSS BOTH LEGS NOW instead of resting one and waiting. See [`Order::cross`]
/// for why, and for the arithmetic that decides when it is worth the spread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cross {
    /// Marketable limit for the leg that would otherwise have rested. The other
    /// leg is re-priced by `price_close` at fill time, exactly as it is after a
    /// resting fill.
    pub limit: String,
    /// What crossing BOTH legs locks per contract, net of BOTH takers' fees.
    /// Always less than [`Order::lock_ct`] — that difference is the spread and
    /// the taker fees, and it is the price of not waiting.
    pub lock_ct: String,
}

/// One exit, priced and ready to rest — or, when [`Order::cross`] is set, to
/// cross.
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub rel_id: String,
    /// WHICH LEG RESTS. Everything below that reads "the resting leg" or "the
    /// close leg" resolves through this and not through the venue names.
    pub shape: Shape,
    /// The Kalshi ticker. The leg this engine is LONG, whichever shape is in
    /// force — the resting ask under [`Shape::RestKalshi`], the fill-time IOC
    /// sell under [`Shape::RestPmUs`].
    pub market: String,
    /// The PM-US market. The leg this engine is SHORT — crossed at fill under
    /// [`Shape::RestKalshi`], rested as a post-only bid under
    /// [`Shape::RestPmUs`].
    pub pm_market: String,
    pub qty: i64,
    /// 4dp, the way the wire wants it.
    pub limit: String,
    /// The `ts` of the open record this closes — ONE record, by construction.
    /// See the module header on why no split is needed.
    pub closes_ts: f64,
    pub k_basis: String,
    pub pm_basis: String,
    /// The OTHER leg's price the exit was priced against — the PM-US ask under
    /// [`Shape::RestKalshi`], the Kalshi bid under [`Shape::RestPmUs`]. Not the
    /// price the close will pay (that is re-derived at fill time), but the
    /// number the decision rested on, so the log line and the record name it.
    pub pm_ask_at_decision: String,
    /// What this exit locks per contract at the price it rested at, against
    /// both legs' ledger basis and net of both legs' fees. The number the shape
    /// contest was decided on, carried so the log line can show its work.
    pub lock_ct: String,
    /// CROSS THIS EXIT INSTEAD OF RESTING IT: the marketable limit for the leg
    /// that would otherwise rest, and the lock crossing still leaves.
    ///
    /// `None` is every exit that has ever been sent — rest a post-only GTC and
    /// wait. `Some` only when `--maker-exit-take` is on AND crossing both legs
    /// still clears [`crate::unwind::MIN_EXIT_CT`] net of BOTH takers' fees.
    ///
    /// WHY IT EXISTS. Resting was not slow, it was ineffective: over the 27 h to
    /// 2026-08-24 the module rested 34 exits and closed NONE, because a 5-lot
    /// passive order joins a queue it cannot get to the front of. Measured on
    /// the live book that day: our `tpoyc-2026-popleo` bid sat THIRD price level
    /// down behind ~711 lots, and our `dontru` bid was at the touch behind
    /// 6,551 lots at the same price. An hour of that is not a fair run at the
    /// queue; it is a coin that never lands.
    ///
    /// The price of certainty is the spread plus the taker fee, and the floor is
    /// what decides whether it is worth paying. On `dontru` that day: 1.00c of
    /// spread and 0.175c of PM-US taker fee (`0.06 * p * (1-p)`, quadratic, so
    /// nearly free at a 3c price) took the lock from 3.4c to ~2.2c — still over
    /// the 2c floor. On `popleo` the same arithmetic gave 0.6c and it is
    /// correctly refused. This is a per-candidate test, never a mode.
    pub cross: Option<Cross>,
    /// The runner-up shape's lock, when it had one. `None` = the other shape
    /// could not be priced at all, and the log line says why instead.
    pub runner_up_ct: Option<String>,
}

impl Order {
    /// The market the post-only order rests on.
    pub fn rest_market(&self) -> &str {
        match self.shape {
            Shape::RestKalshi => &self.market,
            Shape::RestPmUs => &self.pm_market,
        }
    }

    /// The market the fill-time IOC crosses.
    pub fn close_market(&self) -> &str {
        match self.shape {
            Shape::RestKalshi => &self.pm_market,
            Shape::RestPmUs => &self.market,
        }
    }
}

/// The sink the resting order lives at, and the one its fill-time close crosses.
///
/// Both venues were always both present in this loop — the ask rested on one and
/// the IOC crossed the other. All this does is stop the assignment being
/// hardcoded.
fn sinks<'a>(
    shape: Shape,
    kalshi: &'a std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &'a std::sync::Arc<dyn crate::sink::OrderSink>,
) -> (&'a std::sync::Arc<dyn crate::sink::OrderSink>, &'a std::sync::Arc<dyn crate::sink::OrderSink>)
{
    match shape {
        Shape::RestKalshi => (kalshi, pmus),
        Shape::RestPmUs => (pmus, kalshi),
    }
}

/// The (market, side) pair a resting order occupies, for the suppression set.
pub fn rest_key(o: &Order) -> (String, String) {
    match o.shape {
        Shape::RestKalshi => (o.market.clone(), SIDE_ASK.to_string()),
        Shape::RestPmUs => (o.pm_market.clone(), SIDE_BID.to_string()),
    }
}

/// The sides a CROSS takes, which are the OPPOSITE of the ones it might rest.
///
/// **A CROSS THAT HITS OUR OWN QUOTE IS CANCELLED, NOT FILLED.** Kalshi's
/// `self_trade_prevention_type: taker_at_cross` answers a taker that would
/// cross our own resting order by cancelling THE TAKER — the same rule
/// `naked_act` refuses under, and the reason it names a working maker exit as a
/// blocker. The entry quoter rests on both sides across this book (live on
/// 2026-08-24: `kalshi KXTIME-26-POP bid`, `polymarket_us …-flabol bid`,
/// `kalshi KXBRPRES-26-FBOL ask`), and its whole job is to sit AT the touch —
/// which is exactly the price a cross lifts.
///
/// So a cross needs the opposite side yielded for the same reason a rest needs
/// its own, and it would otherwise fail SILENTLY and SAFELY: the taker comes
/// back unfilled and reads as "the book moved", on precisely the markets we
/// quote, which is all of them. Ineffective rather than dangerous, and
/// invisible either way.
pub fn cross_keys(market_id: &str, pm_market: &str) -> Vec<(String, String)> {
    vec![
        (market_id.to_string(), SIDE_BID.to_string()),
        (pm_market.to_string(), SIDE_ASK.to_string()),
    ]
}

/// BOTH sides a candidate might rest, asked for before the shape is known.
///
/// The cycle cannot know which shape wins until it has priced both books, and
/// it may not price them until the quote it would collide with is already
/// yielded — so it yields both and lets [`decide`] check the one it picked.
/// The cost is one extra entry quote suppressed for the settle window on a
/// market we are about to stop quoting anyway; the alternative is a two-cycle
/// handshake that re-decides in between, which is the race this avoids.
pub fn candidate_keys(market_id: &str, pm_market: &str) -> Vec<(String, String)> {
    vec![
        (market_id.to_string(), SIDE_ASK.to_string()),
        (pm_market.to_string(), SIDE_BID.to_string()),
    ]
}

/// What one contract of this exit locks, net of both legs' fees, against both
/// legs' ledger basis. The one arithmetic every shape is scored on.
///
/// TAKES THE ROLES RATHER THAN A [`Shape`] because the CROSS has neither a
/// resting leg nor a shape to name it: both legs pay taker. Passing
/// `shape.roles()` is what the two resting shapes do, so nothing about them
/// changed.
///
/// ```text
///   k_px - fee(kalshi, k_role, k_px) + (1 - pm_px) - fee(pmus, pm_role, pm_px)
///     - k_basis - pm_basis
/// ```
#[allow(clippy::too_many_arguments)]
pub fn lock_per_ct(
    cx: &mut Cx,
    fees: &FeeSchedule,
    (k_role, pm_role): (Role, Role),
    k_px: D,
    pm_px: D,
    k_basis: D,
    pm_basis: D,
    qty: i64,
) -> D {
    let size = cx.from_i64(qty);
    let kf = fees.fee(cx, Venue::Kalshi, k_role, k_px, size, "");
    let kf = cx.div(kf, size);
    let pf = fees.fee(cx, Venue::PolymarketUs, pm_role, pm_px, size, "");
    let pf = cx.div(pf, size);
    let out = cx.sub(k_px, kf);
    let back = cx.one_minus(pm_px);
    let back = cx.sub(back, pf);
    let gross = cx.add(out, back);
    let paid = cx.add(k_basis, pm_basis);
    cx.sub(gross, paid)
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
    take_ok: bool,
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
    // THE OTHER HALF OF THE STAND-OFF, pointing back at us. A post-only ask of
    // ours on a market this engine owes a hedge on makes the ENGINE'S hedge the
    // taker in the collision, and `taker_at_cross` cancels the taker — so the
    // hedge silently does not happen and the leg it was covering stays naked.
    // An ordinary refusal, on the ordinary gauge: this is "not now", not
    // "halted", and the obligation is discharged within seconds.
    crate::naked_act::inflight_check(&cand.market_id).map_err(|why| {
        refuse(format!(
            "not resting an exit on {}: {why}. Our ask would be the maker in that collision \
             and the engine's hedge IOC the taker, which is the order taker_at_cross cancels",
            cand.market_id
        ))
    })?;
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
    // ...AND FROM THE LOT `select` ACTUALLY CHOSE. `cand.opened_ts` is half of
    // the `(rel_id, opened_ts)` key this module already debounces and books
    // against; `lot_at` carries why reading the two legs off THAT record rather
    // than off the dearest of all of them is the difference between an exit
    // that rests inside the book and one that never fills.
    let k_lot = lot_at(
        cx,
        fees,
        records,
        &cand.rel_id,
        cand.opened_ts,
        Venue::Kalshi,
        &cand.market_id,
        Held::LongYes,
    )
    .map_err(|e| refuse(format!("Kalshi leg: {e}")))?;
    let pm_lot = lot_at(
        cx,
        fees,
        records,
        &cand.rel_id,
        cand.opened_ts,
        Venue::PolymarketUs,
        pm_market,
        Held::ShortYes,
    )
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
    let tick = cx.parse_exact(TICK);

    // ------------------------------------------------------ the shape contest ---
    // Both cells with one resting leg are priced, on the SAME basis and the same
    // fee schedule, and the better lock wins. Neither is preferred a priori:
    // gross of fees they differ by `kalshi_spread - pmus_spread`, so which one
    // pays is a property of the two books at this instant and not of the code.

    // A: rest a Kalshi ask; cross the PM-US ask one tick through, matching
    // `mark_positions.py`'s `p_ask + 0.01`.
    let a = (|cx: &mut Cx| -> Result<(D, D), String> {
        let pm_close = cx.add(pm_ask_d, tick);
        let floor = exit_limit(
            cx, fees, &quote.ladder, k_basis, pm_basis, pm_close, qty, Role::Maker, Role::Taker,
            EXIT_FLOOR,
        )?;
        let limit =
            rest_price(cx, &quote.ladder, floor, quote.yes_bid.as_deref(), quote.yes_ask.as_deref())?;
        let limit = cx.quantize_4dp(limit);
        let lock =
            lock_per_ct(cx, fees, Shape::RestKalshi.roles(), limit, pm_close, k_basis, pm_basis, qty);
        Ok((limit, lock))
    })(cx);

    // B: rest a PM-US bid; cross the Kalshi bid one tick under, which is the
    // mirror of A's tick and conservative in the same direction — a marketable
    // sell limit one tick under the bid fills AT the bid or better, so pricing
    // the decision at `bid - TICK` cannot flatter it.
    let b = (|cx: &mut Cx| -> Result<(D, D), String> {
        let Some(k_bid) = quote.yes_bid.as_deref().and_then(|x| cx.parse(x)) else {
            return Err(format!(
                "{} has no Kalshi bid — the leg this shape SELLS into is unpriceable",
                cand.market_id
            ));
        };
        let k_take = cx.sub(k_bid, tick);
        if !cx.is_pos(k_take) {
            return Err(format!(
                "the Kalshi bid is {} — one tick under it is not a price to sell at",
                cx.emit_6dp(k_bid)
            ));
        }
        let ceiling = close_limit(
            cx, fees, k_take, k_basis, pm_basis, qty, Role::Taker, Role::Maker, EXIT_FLOOR,
        )?;
        let pm_bid = view.pm_bid.get(pm_market).map(String::as_str);
        let limit = rest_price_bid(cx, ceiling, pm_bid, Some(pm_ask.as_str()))?;
        let limit = cx.quantize_4dp(limit);
        let lock =
            lock_per_ct(cx, fees, Shape::RestPmUs.roles(), k_take, limit, k_basis, pm_basis, qty);
        Ok((limit, lock))
    })(cx);

    // A shape that cannot clear MIN_LOCK is not a candidate for the contest,
    // whatever the other one does. Priced-but-unprofitable and unpriceable are
    // reported differently because they need different people.
    // THE SAME FLOOR THE SELECTOR USED, on the price we are actually about to
    // send. This used to be `MIN_LOCK` (0.005), which is ported verbatim from
    // `hedge_naked_legs.py` and was chosen for a leg that is ALREADY NAKED and
    // needs out — a case where half a cent beats carrying direction for months.
    // It was never chosen for "should we exit a hedged position at all", and it
    // is four times looser than `MIN_EXIT_CT`, the floor `unwind::consider` had
    // to clear to admit this lot in the first place.
    //
    // The gap was reachable, not theoretical: `consider` screens on the MARKS
    // snapshot and `select` will price off one up to 900 s old, while this
    // re-prices against the book NOW. A lot admitted at 2c could therefore go
    // out at 0.6c on a book that had moved — the exact case of owning at 0.95
    // and selling at 0.96, which the 2c screen refuses and the 0.5c floor let
    // through.
    //
    // `MIN_EXIT_CT` is a NOISE floor, not a profit target, and its own doc says
    // why two ticks: the exit prices against a snapshot and the Kalshi fee term
    // steps by a whole tick at qty=1, so anything clearing by less than that is
    // measurement error. That reasoning is about the arithmetic, not about when
    // it runs, so it applies here exactly as it does there.
    let lock_floor = cx.parse_exact(crate::unwind::MIN_EXIT_CT_S);
    let graded = |cx: &mut Cx, r: &Result<(D, D), String>| -> Result<(D, D), String> {
        match r {
            Ok((limit, lock)) if cx.cmp(*lock, lock_floor) != Ordering::Less => Ok((*limit, *lock)),
            Ok((limit, lock)) => Err(format!(
                "priced at {} but locks only {}/ct, under the {}/ct floor the selector \
                 admitted it under",
                cx.emit_6dp(*limit),
                cx.emit_6dp(*lock),
                crate::unwind::MIN_EXIT_CT_S
            )),
            Err(e) => Err(e.clone()),
        }
    };
    let ga = graded(cx, &a);
    let gb = graded(cx, &b);
    let (shape, limit, lock, runner_up) = match (&ga, &gb) {
        (Err(ea), Err(eb)) => {
            return Err(refuse(format!(
                "neither exit shape pays on {}. rest-kalshi: {ea}. rest-pmus: {eb}",
                cand.rel_id
            )))
        }
        (Ok((l, lk)), Err(_)) => (Shape::RestKalshi, *l, *lk, None),
        (Err(_), Ok((l, lk))) => (Shape::RestPmUs, *l, *lk, None),
        (Ok((la, ka)), Ok((lb, kb))) => {
            // Ties go to RestKalshi: it is the shape with the longer live
            // record, and a tie is not evidence for changing venue.
            if cx.cmp(*kb, *ka) == Ordering::Greater {
                (Shape::RestPmUs, *lb, *kb, Some(*ka))
            } else {
                (Shape::RestKalshi, *la, *ka, Some(*kb))
            }
        }
    };

    // THE SUPPRESSION GATE, on the side this shape actually rests. The cycle
    // asks for BOTH sides before deciding — it cannot know which shape will win
    // until the books are priced — so whichever wins here has already been
    // yielded for the same settle window.
    let rest_key = match shape {
        Shape::RestKalshi => (cand.market_id.clone(), SIDE_ASK.to_string()),
        Shape::RestPmUs => (pm_market.to_string(), SIDE_BID.to_string()),
    };
    match view.suppressed_at.get(&rest_key) {
        None => {
            return Err(refuse(format!(
                "the entry quoter has not yet been told to yield {}:{} — asked for it this \
                 cycle; nothing rests until it has (unwind §5, card ed6a5910)",
                rest_key.0, rest_key.1
            )))
        }
        Some(since) if now.saturating_duration_since(*since).as_secs_f64() < SUPPRESS_SETTLE_S => {
            return Err(refuse(format!(
                "{}:{} was yielded {:.0}s ago; giving it {SUPPRESS_SETTLE_S:.0}s to cancel \
                 before resting against it",
                rest_key.0,
                rest_key.1,
                now.saturating_duration_since(*since).as_secs_f64()
            )))
        }
        Some(_) => {}
    }

    let against = match shape {
        Shape::RestKalshi => pm_ask.clone(),
        Shape::RestPmUs => quote.yes_bid.clone().unwrap_or_default(),
    };

    // ------------------------------------------------------------- the cross ---
    // CROSSING IS ONE ECONOMIC ACTION AND SO ONE NUMBER, whichever shape won the
    // contest above: sell the Kalshi YES into its bid, buy the PM-US YES at its
    // ask, both taker. The shape still decides which leg goes FIRST — that is
    // `rest_market`/`close_market` and the sides `place` and `close_leg` read —
    // but it does not change what crossing pays, so this is computed once.
    //
    // Priced with the SAME conservative ticks the resting shapes use: a tick
    // UNDER the Kalshi bid (a marketable sell fills at the bid or better, so
    // this cannot flatter it) and a tick THROUGH the PM-US ask. Crossing is
    // therefore scored against prices at least as bad as the ones it will get.
    //
    // AGAINST `MIN_EXIT_CT`, NOT `MIN_LOCK`. The placement floor is 0.005 and
    // the selection floor is 0.02, and paying the spread has to clear the harder
    // one: `unwind::consider` already refused this lot at anything under 0.02 as
    // not worth doing, and a cross that dropped below it would be spending the
    // spread to reach a trade the selector had rejected.
    let cross = take_ok
        .then(|| {
            // THE SIDE A CROSS LIFTS MUST BE YIELDED TOO, and it is the opposite
            // of the one checked above: `taker_at_cross` cancels a taker that
            // would hit our own resting quote, so an unyielded side does not
            // make the cross wrong, it makes it a no-op that reads as "the book
            // moved". Same settle window as the rest guard, for the same reason.
            let lift = match shape {
                Shape::RestKalshi => (cand.market_id.clone(), SIDE_BID.to_string()),
                Shape::RestPmUs => (pm_market.to_string(), SIDE_ASK.to_string()),
            };
            match view.suppressed_at.get(&lift) {
                Some(since)
                    if now.saturating_duration_since(*since).as_secs_f64()
                        >= SUPPRESS_SETTLE_S => {}
                // Not yielded, or not yet settled: rest this cycle rather than
                // send a taker into our own book. The candidate comes back round
                // and crosses then, by which time the yield has landed.
                _ => return None,
            }
            let k_bid = quote.yes_bid.as_deref().and_then(|x| cx.parse(x))?;
            let k_take = cx.sub(k_bid, tick);
            if !cx.is_pos(k_take) {
                return None;
            }
            let pm_take = cx.add(pm_ask_d, tick);
            let lock = lock_per_ct(
                cx,
                fees,
                (Role::Taker, Role::Taker),
                k_take,
                pm_take,
                k_basis,
                pm_basis,
                qty,
            );
            let floor = cx.parse_exact(crate::unwind::MIN_EXIT_CT_S);
            if cx.cmp(lock, floor) == Ordering::Less {
                return None;
            }
            // The leg that would have rested is the one crossed FIRST, at its
            // marketable price; the other is closed by `close_leg` exactly as it
            // is after a resting fill, re-priced against the book at that moment.
            let first = match shape {
                Shape::RestKalshi => k_take,
                Shape::RestPmUs => pm_take,
            };
            Some(Cross {
                limit: cx.quantize_4dp(first).to_standard_notation_string(),
                lock_ct: cx.emit_6dp(lock),
            })
        })
        .flatten();

    Ok(Order {
        rel_id: cand.rel_id.clone(),
        shape,
        market: cand.market_id.clone(),
        pm_market: pm_market.to_string(),
        qty,
        limit: limit.to_standard_notation_string(),
        closes_ts: k_lot.open_ts,
        k_basis: k_lot.cost_per_ct,
        pm_basis: pm_lot.cost_per_ct,
        pm_ask_at_decision: against,
        lock_ct: cx.emit_6dp(lock),
        cross,
        runner_up_ct: runner_up.map(|r| cx.emit_6dp(r)),
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
/// `naked_act::close_record`, where only one did — but the PM-US FEES are not
/// on anything this process reads, so `fees_pending` says what
/// `engine::fill::book_basket` says and for the same reason.
///
/// THE PRICES ARE THE ONES WE ACTUALLY GOT, and that sentence sat here being
/// false for the whole armed life of this module. Callers passed the LIMITS
/// they had sent. A marketable limit is a floor — "do not fill me worse than
/// this" — and both venues fill it at the touch when the touch is better, so
/// the recorded price was systematically worse than the trade. Across 30 closes
/// it understated Kalshi proceeds by $9.71, which is the difference between
/// this programme reading as +$8.58 of profit and as a $0.91 loss. Callers now
/// read `order_fill` from the venue and pass that; see `fill_price` for the two
/// guards that decide when a venue's number may be believed.
///
/// KALSHI'S SETTLED FEE IS DELIBERATELY NOT RECORDED even though the same read
/// returns it. `trades::legs` marks a record's fees SETTLED when ANY leg
/// carries one, so a Kalshi-only fee would make the tab claim the whole
/// record's fees are bank truth while the PM-US half is still modelled. Both or
/// neither.
/// The two fills mapped onto the venues that paid them. ONE PLACE, so the
/// ledger record and the log line that announces it cannot disagree about which
/// leg cost what — they did, and nothing caught it.
///
/// Callers hold these by ROLE: the leg the shape rested, and the leg closed by
/// IOC afterwards. Under `rest-kalshi` that is already (kalshi, pmus); under
/// `rest-pmus` it is reversed, and writing them straight onto the legs swapped
/// the two prices on every record of the shape the contest picks most often.
///
/// PURELY THE SHAPE'S MAPPING NOW. It used to also swap in [`Cross::limit`] for
/// the resting leg, because on a cross that leg went out as an IOC and
/// `Order::limit` was never sent to anyone. Callers ask [`Order::rest_limit`]
/// for that instead, so this function does one thing; and in any case a LIMIT
/// is no longer what a caller passes when the venue will tell it the fill.
fn fills_by_venue<'a>(o: &'a Order, rest_fill: &'a str, close_fill: &'a str) -> (&'a str, &'a str) {
    match o.shape {
        Shape::RestKalshi => (rest_fill, close_fill),
        Shape::RestPmUs => (close_fill, rest_fill),
    }
}

impl Order {
    /// The price the RESTING leg was actually sent at. On a cross it never
    /// rested: it went out as an IOC at [`Cross::limit`], and `Order::limit` is
    /// the price a rest WOULD have used and was sent to nobody.
    pub fn rest_limit(&self) -> &str {
        self.cross.as_ref().map_or(self.limit.as_str(), |c| c.limit.as_str())
    }
}

pub fn close_record(o: &Order, rest_fill: &str, close_fill: &str, filled: i64, ts: f64) -> Value {
    // BY LEG ROLE, THEN MAPPED TO VENUES BY THE SHAPE. These used to be named
    // `k_fill`/`pm_fill` and written straight onto the Kalshi and PM-US legs,
    // while all three callers passed them as (the leg that RESTED, the leg that
    // CLOSED). Under `rest-kalshi` those coincide and the record was right;
    // under `rest-pmus` they are reversed and EVERY RECORD HAD ITS TWO PRICES
    // SWAPPED.
    //
    // Caught on 2026-08-25 against the log that placed it: the 08-23 exit rested
    // a PM-US bid `@ 0.0200 on polymarket_us`, and its record carries 0.0200 on
    // the KALSHI leg. `rest-pmus` is the shape the contest picks most often, so
    // this is most of the unwound records in `data/exec/trades.jsonl`.
    let (k_fill, pm_fill) = fills_by_venue(o, rest_fill, close_fill);
    // A CROSS PAYS TAKER ON BOTH LEGS. The shape still says which leg went
    // first, but neither rested, so neither is a maker — and the comment below
    // is exactly why that matters.
    let (k_role, pm_role) = match (o.cross.is_some(), o.shape) {
        (true, _) => ("taker", "taker"),
        (false, Shape::RestKalshi) => ("maker", "taker"),
        (false, Shape::RestPmUs) => ("taker", "maker"),
    };
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
        "maker_exit_shape": o.shape.tag(),
        // The lock that was actually TAKEN. A cross pays the spread and both
        // takers' fees to convert now, so it locks strictly less than the rest
        // it replaced — recording the rested figure would book a profit this
        // trade did not make.
        "maker_exit_lock_ct": o.cross.as_ref().map_or(&o.lock_ct, |c| &c.lock_ct),
        "note": match &o.cross {
            Some(c) => format!(
                "opportunistic maker exit ({}): BOTH legs were CROSSED — an IOC on {} at {} \
                 rather than a post-only order rested there — because crossing still locked \
                 {}/ct against BOTH legs' ledger basis, net of both takers' fees, where \
                 resting would have locked {}/ct and waited. The difference is the spread and \
                 the taker fees, and it is what was paid to convert now instead of joining a \
                 queue. Sized to ONE open lot, so this closes exactly one record. The leg \
                 prices are the venues' OWN fills, not the limits sent — an IOC fills at the \
                 touch when the touch is better, and the difference is real money. \
                 realized_pnl_usd is still absent: PM-US fees are not on anything this process \
                 reads, and half-settled fees would read as fully settled downstream.",
                o.shape.tag(),
                o.shape.rest_venue().as_str(),
                c.limit,
                c.lock_ct,
                o.lock_ct
            ),
            None => format!(
                "opportunistic maker exit ({}): a post-only order rested on {} at the price \
                 that locked a profit against BOTH legs' ledger basis, and the other leg was \
                 closed with an IOC re-priced against the book at fill time. Sized to ONE open \
                 lot, so this closes exactly one record. The leg prices are the venues' OWN \
                 fills, not the limits sent — an IOC fills at the touch when the touch is \
                 better, and the difference is real money. realized_pnl_usd is still absent: \
                 PM-US fees are not on anything this process reads, and half-settled fees would \
                 read as fully settled downstream.",
                o.shape.tag(),
                o.shape.rest_venue().as_str()
            ),
        },
        // THE ROLES ARE THE SHAPE'S, not a fixed property of the venue. Under
        // `rest-kalshi` the Kalshi leg is the maker and PM-US the taker; under
        // `rest-pmus` it is the other way round, and a record that said
        // otherwise would misattribute the fees this file exists to account for.
        "legs": [
            {"venue": "kalshi", "market_id": o.market, "side": "yes", "action": "sell",
             "role": k_role, "qty": filled, "yes_price": k_fill},
            {"venue": "polymarket_us", "market_id": o.pm_market, "side": "no", "action": "sell",
             "role": pm_role, "qty": filled, "yes_price": pm_fill},
        ],
    })
}


// ------------------------------------------------------ what it actually got ---

/// Which way a leg's price runs for the venue it is on, for the guard below.
/// The KALSHI leg of an exit always SELLS the YES; the PM-US leg always BUYS it
/// back (buying the YES against a NO long is how PM-US flattens one — there is
/// no sell intent on that wire). So this is a property of the VENUE, not of
/// which leg the shape happened to rest.
pub(crate) fn sells_yes(v: Venue) -> bool {
    matches!(v, Venue::Kalshi)
}

/// The price a leg ACTUALLY filled at, YES-side — or `None`, meaning "keep the
/// limit", which is what every booking path here did unconditionally until now.
///
/// A LIMIT IS A FLOOR, NOT A PREDICTION. It says "do not fill me worse than
/// this"; a CLOB fills it at the touch when the touch is better, and both
/// venues do. Booking the limit understated Kalshi proceeds by $9.71 across 30
/// closes — enough to report +$8.58 of realized profit as a $0.91 loss, and to
/// retire a strategy that was working.
///
/// GUARDED, and the guard is the whole reason this may be trusted into the
/// ledger. A fill can never be WORSE than the limit that authorised it: a buy
/// cannot pay more, a sell cannot receive less. A number that says otherwise is
/// not a windfall, it is a conversion this file got wrong — an inverted
/// complement, a `no`-side quote, a units change — and the ledger is where the
/// basis of every FUTURE exit is folded from, so a wrong price there is not one
/// bad row. It refuses to the limit, which is exactly today's behaviour.
pub(crate) fn fill_price(cx: &mut Cx, f: &OrderFill, limit: &str, selling: bool) -> Option<String> {
    let px = match f {
        OrderFill::Notional(fc) => {
            let qty = cx.parse(&fc.qty)?;
            if !cx.is_pos(qty) {
                return None;
            }
            let (t, m) = (cx.parse(&fc.taker_cost_usd)?, cx.parse(&fc.maker_cost_usd)?);
            let cost = cx.add(t, m);
            let per = cx.div(cost, qty);
            // Kalshi quotes the notional of the side the order ended up LONG,
            // and selling a YES is buying its NO.
            if fc.complement {
                cx.one_minus(per)
            } else {
                per
            }
        }
        OrderFill::Average { avg_px, .. } => cx.parse(avg_px)?,
    };
    // A price outside (0, 1) is not a probability and cannot be a fill.
    let one = cx.parse_exact("1");
    if !cx.is_pos(px) || cx.cmp(px, one) != Ordering::Less {
        return None;
    }
    let lim = cx.parse(limit)?;
    let ok = if selling {
        cx.cmp(px, lim) != Ordering::Less
    } else {
        cx.cmp(px, lim) != Ordering::Greater
    };
    ok.then(|| cx.quantize_4dp(px).to_standard_notation_string())
}

/// What the two legs together claim, bounded by what a flattened basket can
/// possibly pay.
///
/// THE SECOND HALF OF THE GUARD, and the half that catches an inverted read.
/// `fill_price`'s "never worse than its limit" is one-sided: a sell read with
/// the complement the wrong way round comes out BETTER than its limit and sails
/// through. But the two legs of an exit close ONE hedged basket, and a hedged
/// basket pays exactly $1.00 a contract — so `k + (1 - pm)`, the gross proceeds
/// of flattening it, has to land near a dollar whatever the prices were.
///
/// The band is deliberately loose. This is a bound on NONSENSE, not a re-pricing
/// and not an edge check: a genuinely bad exit is still recorded, because
/// recording bad news correctly is the entire point of this change. An inverted
/// leg misses by 50c or more and every real close on disk sits inside 0.95-1.05.
fn plausible_pair(cx: &mut Cx, k_px: &str, pm_px: &str) -> bool {
    let (Some(k), Some(pm)) = (cx.parse(k_px), cx.parse(pm_px)) else { return false };
    let back = cx.one_minus(pm);
    let gross = cx.add(k, back);
    let (lo, hi) = (cx.parse_exact("0.5"), cx.parse_exact("1.5"));
    cx.cmp(gross, lo) == Ordering::Greater && cx.cmp(gross, hi) == Ordering::Less
}

/// Ask the venue what one order filled at, and fall back to its limit.
///
/// Never an error path: a venue that will not answer leaves the record exactly
/// as good as it was before this existed, and the order is already spent either
/// way. Failing the close over a PRICE READ would be strictly worse than
/// booking the conservative number.
///
/// `selling` is passed rather than derived from the venue because it is NOT a
/// property of the venue outside this file. Here every Kalshi leg sells its YES
/// and every PM-US leg buys one back, which is what [`sells_yes`] encodes — but
/// `positions::place_and_book` shares this function and goes BOTH ways on
/// Kalshi: it buys the missing YES to complete a naked PM short, and sells an
/// excess YES to close one. The guard inside [`fill_price`] is "never worse than
/// the limit", which inverts with the direction, so a caller that got this wrong
/// would silently keep its limit on every genuinely better fill.
pub(crate) async fn filled_price_or_limit(
    cx: &mut Cx,
    sink: &std::sync::Arc<dyn crate::sink::OrderSink>,
    order_id: &str,
    limit: &str,
    selling: bool,
) -> String {
    let s = sink.clone();
    let id = order_id.to_string();
    match tokio::task::spawn_blocking(move || s.order_fill(&id)).await {
        Ok(Ok(Some(f))) => {
            fill_price(cx, &f, limit, selling).unwrap_or_else(|| limit.to_string())
        }
        _ => limit.to_string(),
    }
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

/// A Kalshi exit that SOLD and whose PM-US close did not complete.
///
/// The old code had nowhere to put this: every failure arm called
/// `alarm_unresolved` and returned, so the only record that we were one-legged
/// was a log line and a counter, and the only recovery was a human. This is that
/// state made durable enough to act on.
pub struct PendingClose {
    /// The exit whose Kalshi leg is already gone. Carries the markets, the two
    /// bases and the `closes_ts` the eventual `unwound` record must name.
    pub order: Order,
    /// Contracts the Kalshi ask sold. The PM-US side owes exactly this many.
    pub filled: i64,
    /// When the Kalshi fill happened — how long we have been naked.
    pub since: Instant,
    /// Cycles [`heal`] has tried. Past [`HEAL_PROFITABLE_CYCLES`] it stops
    /// insisting the close be profitable.
    pub attempts: u32,
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
    /// `--maker-exit-take`: may an exit CROSS both legs when doing so still
    /// clears the selection floor? Off unless asked for. See [`Order::cross`].
    pub take_ok: bool,
    pub ledger_path: String,
    pub debounce: Debounce,
    pub resting: Option<Resting>,
    /// A close that did not complete, carried across cycles so [`heal`] can
    /// finish it. `Some` IS the latch's cause; [`outstanding`] is its effect.
    pub pending: Option<PendingClose>,
    /// Why the resting exit should give up the single [`MAX_RESTING`] slot, when
    /// it should. Set by [`Live::target`] — which is the only place that both
    /// knows the age of the resting ask and has the admitted candidate set to
    /// judge whether the slot is contested — and consumed by [`cycle`], which
    /// owns the wire. See [`MAX_RESTING_S`].
    pub rotate: Option<String>,
    /// Markets that have ALREADY HELD the single [`MAX_RESTING`] slot during the
    /// current rotation, oldest first. Selection skips them, so the slot goes
    /// round the held set instead of back to whoever `select` ranks first.
    ///
    /// A rotation that hands the slot straight back to the market it took it
    /// from has bought that market's own queue position at full price, which is
    /// the loss [`MAX_RESTING_S`] says it is not willing to pay. `target` orders
    /// candidates by `select`'s ordering and takes the first, and the market
    /// that has been resting longest is very often still first — so on
    /// 2026-08-20 `KXTIME-26-AI` was rotated out at 11:46 and rested again,
    /// same price, at 11:50.
    ///
    /// **EXCLUDING ONLY THE INCUMBENT IS NOT A ROTATION**, which is what this
    /// was and why it is now a list. One name is enough to stop an immediate
    /// re-place and not enough to reach the third candidate: `select`'s
    /// ordering is stable, so excluding the incumbent hands the slot to the
    /// same runner-up every time and the two of them trade it for ever. Live
    /// over the 27 h to 2026-08-24 20:00 that is exactly what happened — 31
    /// exits placed, 0 filled, the slot ping-ponging between
    /// `tac-nobel-peace-…-dontru`, `cpc-btc-…-150k` and `tpoyc-2026-ai` while
    /// the rotation's own log line reported "6 other held candidate(s)
    /// waiting", then "8 other". Those never got a turn, and the topics they
    /// would have freed (`time-poty-26` at 185/185) are the ones refusing every
    /// new basket.
    ///
    /// Cleared when every live candidate has had the slot — a completed lap,
    /// after which the whole set is eligible again. See [`HANDOVER_CYCLES`] for
    /// the bound that keeps a candidate nobody can price from locking the rest
    /// out.
    served: Vec<String>,
    /// Selections since anything last RESTED, while a lap is in progress.
    /// Reaching [`HANDOVER_CYCLES`] re-admits the longest-excluded market
    /// rather than clearing the lap — never merely because a selection read it.
    handover_age: u32,
    /// Markets where a place or a cancel DID NOT COMPLETE, so an ask of ours may
    /// be resting under an id nothing in this process can address.
    ///
    /// NEVER REMOVED IN-PROCESS, for [`UNRESOLVED`]'s reason: nothing here can
    /// learn that the order is gone. It holds the stand-off on that market until
    /// a restart, which costs the naked-leg backstop a market it might have
    /// completed — the safe direction, because the alternative is a taker of
    /// ours crossing an ask of ours and being cancelled for it.
    unaddressable: BTreeSet<String>,
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
            take_ok: false,
            ledger_path,
            debounce: Debounce::default(),
            resting: None,
            pending: None,
            rotate: None,
            served: Vec::new(),
            handover_age: 0,
            unaddressable: BTreeSet::new(),
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

    /// The markets this module is working, for [`publish_working`]: what it can
    /// no longer address, plus what it has resting, plus the one it is about to
    /// rest on.
    fn working_set(&self, extra: Option<&str>) -> BTreeSet<String> {
        let mut s = self.unaddressable.clone();
        if let Some(r) = &self.resting {
            s.insert(r.order.market.clone());
        }
        if let Some(m) = extra {
            s.insert(m.to_string());
        }
        s
    }

    /// Which candidate, if any, this cycle may work — the gate that makes
    /// "rest a second ask" unexpressible.
    ///
    /// `Err` carries the reason so the operator can tell a quiet book from a
    /// held one; `Ok(None)` never happens, which is why there is no third arm.
    ///
    /// `unresolved` is [`unresolved()`] PASSED IN rather than read here, and
    /// that is not style: the counter is process-wide and never decrements, so a
    /// gate reading it directly would make every test of this function depend on
    /// which other test alarmed first.
    pub fn target<'a>(
        &mut self,
        exits: &'a [crate::unwind::Exit],
        now: f64,
        outstanding: u64,
    ) -> Result<&'a crate::unwind::Exit, String> {
        // The debounce is folded on EVERY cycle, including one that will not
        // place: forgetting a scan because an exit was already resting would
        // restart the clock for every other candidate.
        let admitted = self.debounce.admit(exits, now);
        if outstanding > 0 {
            // NOT `refuse()`. The REFUSED gauge is the resting state of a module
            // that mostly declines — "the book did not pay" — and this is a
            // halt: an exit of ours filled and its close did not, so the ledger
            // quantity anything here would price against has ALREADY been sold
            // at the venue. See the gauge's own doc for why the two must never
            // share a number.
            return Err(format!(
                "{outstanding} naked leg(s) outstanding — a Kalshi exit SOLD contracts whose \
                 PM-US leg this process could not close or could not read, so the lot the \
                 ledger still calls open is one the venue has already sold. Resting another \
                 ask against it compounds the naked short by a clip a cycle, which is the \
                 failure this halt exists to make unexpressible. `heal` IS WORKING ON IT every \
                 cycle — re-reading venue truth, re-sizing to the shortfall and re-pricing — \
                 and this clears the moment it is flat; no restart, and no hand. ({} of {} \
                 candidate(s) are still held, and the debounce is still being folded for all \
                 of them.)",
                admitted.len(),
                exits.len()
            ));
        }
        if let Some(r) = self.resting.as_ref() {
            // THE SLOT IS THE SCARCE RESOURCE, and until now nothing rationed it:
            // an ask that never filled held the recycler shut for as long as it
            // cared to. Arm a rotation once it is older than `MAX_RESTING_S` AND
            // somebody else would actually use the slot — a held candidate on a
            // DIFFERENT market. Rotating for the sake of the exit's own candidate
            // would just buy back its own queue position at full price.
            let age = r.since.elapsed().as_secs_f64();
            // ...AND THE SOMEBODY ELSE HAS TO BE ABLE TO USE IT. A candidate on
            // a market the venue has halted cannot take the slot, so counting it
            // as a waiter buys a cancel and hands the slot back — which is how
            // `KXFRENCHPRES-27-BRET`, halted, cost `KXTIME-26-AI` its queue
            // position twice on 2026-08-20 before the park took hold.
            let waiting = admitted
                .iter()
                .filter(|e| e.market_id != r.order.market && !self.is_parked(&e.market_id))
                .count();
            let incumbent = r.order.market.clone();
            self.rotate = (age > MAX_RESTING_S && waiting > 0).then(|| {
                format!(
                    "{incumbent} has held the only exit slot for {age:.0}s (limit \
                     {MAX_RESTING_S:.0}s) with {waiting} other held candidate(s) waiting on \
                     it — pulling so one of them can have it. {incumbent} is skipped until \
                     every other held candidate has had the slot; it comes back round on the \
                     lap after."
                )
            });
            if self.rotate.is_some() {
                if !self.served.contains(&incumbent) {
                    self.served.push(incumbent);
                }
                self.handover_age = 0;
            }
            return Err(format!(
                "an exit is already resting ({} of {} in-scope candidate(s) held) — \
                 MAX_RESTING is {MAX_RESTING}, so nothing else may rest until it fills or is \
                 pulled",
                admitted.len(),
                exits.len()
            ));
        }
        if admitted.is_empty() {
            return Err(format!(
                "{} candidate(s), none held for {DEBOUNCE_S:.0}s across {DEBOUNCE_SCANS} \
                 scans — 60% of every excursion above the floor on the live tape is one \
                 sample long",
                exits.len()
            ));
        }
        // Parked markets are filtered rather than tripped over. The old code
        // took the first admitted candidate and THEN refused if it was halted,
        // which let one halted market shadow every live one behind it.
        let live: Vec<&crate::unwind::Exit> =
            admitted.iter().copied().filter(|e| !self.is_parked(&e.market_id)).collect();
        if live.is_empty() {
            let halted: Vec<&str> = admitted.iter().map(|e| e.market_id.as_str()).collect();
            return Err(format!(
                "all {} held candidate(s) are on markets halted at the venue ({}); not \
                 re-sending into them",
                admitted.len(),
                halted.join(", ")
            ));
        }
        // THE HAND-OVER, which survives until somebody else actually RESTS or
        // [`HANDOVER_CYCLES`] selections have gone by — NOT until the first
        // selection reads it. Reading it is not evidence that it worked: the
        // cycle that first picks a market always refuses, because it is the
        // cycle that asks for the entry quote to be yielded. See the constant.
        //
        // BOUNDED, because a challenger nobody can ever price would otherwise
        // hold the slot empty for good, and an empty slot earns nothing at all
        // while the incumbent's ask at least pays if somebody lifts it. The
        // budget re-admits the LONGEST-EXCLUDED market rather than abandoning
        // the lap: one name comes back, the rest of the queue is still owed a
        // turn.
        if !self.served.is_empty() {
            self.handover_age += 1;
            if self.handover_age > HANDOVER_CYCLES {
                self.served.remove(0);
                self.handover_age = 0;
            }
        }
        // A LAP, not a swap. The first candidate that has not yet held the slot
        // this lap takes it; when every live one has, the lap is complete and
        // the whole set becomes eligible again.
        //
        // If everything live has already been served — including the case where
        // the rotated-out market is the only candidate at all — the lap resets
        // and `live[0]` is chosen again. There was nobody to hand to, and an
        // empty slot is worse than a re-bought queue position. That was true
        // before and is unchanged.
        let unserved = live
            .iter()
            .copied()
            .find(|e| !self.served.contains(&e.market_id));
        let e = match unserved {
            Some(e) => e,
            None => {
                self.served.clear();
                self.handover_age = 0;
                live[0]
            }
        };
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
    let (verb, against) = match o.shape {
        Shape::RestKalshi => ("REST ASK", "pm ask"),
        Shape::RestPmUs => ("REST BID", "kalshi bid"),
    };
    // The runner-up is shown whenever there was one, because "we chose this
    // venue" is only meaningful next to what the other one offered. A shape
    // that could not be priced at all says so in the refusal line instead.
    let versus = match &o.runner_up_ct {
        Some(r) => format!(", beating the other shape's {r}/ct"),
        None => " (the other shape could not be priced)".to_string(),
    };
    format!(
        "[maker-exit]{} {verb} {}x {} @ {} on {} [{}] (closes the lot opened at ts {}, kalshi \
         basis {}/ct + pm basis {}/ct, {against} {} at decision, locks {}/ct{versus}; held \
         {:.0}s) — id {}",
        if shadow { " SHADOW —" } else { "" },
        o.qty,
        o.rest_market(),
        o.limit,
        o.shape.rest_venue().as_str(),
        o.shape.tag(),
        o.closes_ts,
        o.k_basis,
        o.pm_basis,
        o.pm_ask_at_decision,
        o.lock_ct,
        held_s,
        o.rel_id,
    )
}

/// Book a filled-and-closed exit. Through `append_basket`, never a raw append:
/// the single-author rule is what caught the 2026-07-30 double-book.
/// `rest_fill` is the price of the leg the shape RESTED (or, on a cross, would
/// have); `close_fill` the leg closed by IOC afterwards. Named by ROLE because
/// that is how every caller has them — mapping them onto venues is
/// [`close_record`]'s job and getting it wrong there swapped the two prices on
/// every `rest-pmus` record until 2026-08-25.
pub fn book(path: &str, o: &Order, rest_fill: &str, close_fill: &str, filled: i64, ts: f64) -> String {
    let rec = close_record(o, rest_fill, close_fill, filled, ts);
    let (k_fill, pm_fill) = fills_by_venue(o, rest_fill, close_fill);
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
/// Alarm on a naked leg AND hand it to [`heal`].
///
/// One function, because the two must not be able to drift. A leg that alarms
/// and is not parked waits for a human — the old behaviour, and the defect. A
/// leg that is parked and does not alarm is worse: it would self-heal silently,
/// so nobody would learn the close path had failed at all. Every arm does both.
fn park(live: &mut Live, o: &Order, qty: i64, since: Instant, why: &str) -> String {
    let line = alarm_unresolved(o, qty, why);
    live.pending = Some(PendingClose { order: o.clone(), filled: qty, since, attempts: 0 });
    line
}

pub fn alarm_unresolved(o: &Order, filled: i64, why: &str) -> String {
    UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
    // THE SIDE WE ARE NAKED ON IS THE SHAPE'S, and this is the line a human
    // reads at 3am. Under `rest-kalshi` the Kalshi YES has been SOLD and the
    // PM-US short is uncovered; under `rest-pmus` the PM-US short has been
    // BOUGHT BACK and the Kalshi LONG is uncovered. They are opposite
    // positions, they need opposite corrections, and a message that named the
    // wrong one would send the reader to hedge the wrong way round.
    let (done, exposure, recon) = match o.shape {
        Shape::RestKalshi => (
            format!("{filled}x {} SOLD on Kalshi and the PM-US close on {} did NOT complete",
                    o.market, o.pm_market),
            format!("short {filled} PM-US YES with nothing against it"),
            "Do NOT also let --positions-recon-act complete it by BUYING KALSHI BACK — that \
             re-opens what this just exited",
        ),
        Shape::RestPmUs => (
            format!("{filled}x {} BOUGHT BACK on PM-US and the Kalshi sell on {} did NOT \
                     complete", o.pm_market, o.market),
            format!("long {filled} Kalshi YES with nothing against it"),
            "Do NOT also let --positions-recon-act complete it by SELLING PM-US SHORT again — \
             that re-opens the leg this just closed",
        ),
    };
    format!(
        "[maker-exit] ### NAKED AFTER EXIT ### {done} ({why}). We are {exposure}, and the \
         ledger still says the basket opened at ts {} is OPEN — so no exposure fold, no cap \
         and no unwind can see this. `heal` HAS IT: from the next 60s cycle it re-reads that \
         venue's truth, sizes the retry to the shortfall and re-prices, profitable-only for \
         {HEAL_PROFITABLE_CYCLES} cycles and then crossing out regardless. NO HAND NEEDED \
         unless it is still saying this in ~15 minutes. ({recon}; it is profitable-only and \
         has acted 0 times to date, so in practice it will not.) maker_exit_unresolved is now \
         {}.",
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
    // WHAT WE ARE WORKING, BEFORE ANYTHING CAN RETURN. Published first so that
    // every early return below — an unusable view, a failed scan, a held
    // target — leaves the backstop reading a set this cycle actually built,
    // and so that a market this module has just given up on is given up on
    // within one cycle rather than at the next successful place.
    publish_working(live.working_set(None));
    let view = match engine_view() {
        Ok(v) => v,
        Err(why) => {
            // A view we cannot believe is not a reason to leave an ask resting
            // against it. PULL IT. The engine stops publishing when it is killed
            // or feed-pulled, so this is also how a halted engine retires an
            // exit that it can no longer value.
            if live.resting.is_some() {
                out.push(format!("[maker-exit] engine view unusable ({why}) — pulling the ask"));
                out.extend(pull(live, kalshi, pmus).await);
            }
            request_suppress(BTreeSet::new());
            return out;
        }
    };
    // HEAL FIRST OF ALL. A pending close is a NAKED LEG — a real directional
    // position, already at the venue, that nothing else here can see. It
    // outranks a resting ask for the same reason a resting ask outranks a new
    // candidate, only more so, and while it is outstanding `Live::target`
    // refuses everything anyway.
    if live.pending.is_some() {
        out.extend(heal(live, &view, kalshi, pmus).await);
    }
    // MANAGE NEXT. A resting ask is money already at a venue; a new candidate
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
            request_suppress(live.resting.iter().map(|r| rest_key(&r.order)).collect());
            return out;
        }
    };
    let now = wall_now();
    let target = match live.target(&exits, now, outstanding()) {
        Ok(e) => e.clone(),
        Err(why) => {
            out.push(format!("[maker-exit] nothing to rest: {why}"));
            // ...but the slot may have been held too long by an ask nothing is
            // going to lift. Pull it HERE rather than inside `target`, which is
            // pure and does not own the wire. The next cycle then re-decides
            // from scratch, this one rests nothing — the same one-cycle gap
            // every other decision here takes.
            if let Some(reason) = live.rotate.take() {
                out.push(format!("[maker-exit] ROTATING — {reason}"));
                out.extend(pull(live, kalshi, pmus).await);
            }
            request_suppress(live.resting.iter().map(|r| rest_key(&r.order)).collect());
            return out;
        }
    };
    // THE ENTRY QUOTE COMES OFF FIRST. Published before the decision, so the
    // first cycle that picks a market ASKS and the next one places — which is
    // exactly what `decide`'s settle guard refuses on, by name.
    let mut want: BTreeSet<(String, String)> =
        live.resting.iter().map(|r| rest_key(&r.order)).collect();
    // ...and the backstop is stood off the same market in the same breath,
    // BEFORE any wire call. The window this closes is one cycle wide: the
    // publication at the top of this cycle did not know which candidate would be
    // picked, and the recon pass runs on its own 5-minute timer inside this
    // process.
    publish_working(live.working_set(Some(&target.market_id)));

    let Some(pm) = cfg.pm_market.get(&target.rel_id).cloned() else {
        out.push(refuse(format!(
            "[maker-exit] NO — {} has no polymarket_us leg in the registry, so there is no \
             close leg to price",
            target.rel_id
        )));
        // Nothing about this target may rest, but whatever IS resting still
        // must keep its quote yielded.
        request_suppress(want);
        return out;
    };
    // BOTH sides this candidate might rest, published before the decision — the
    // first cycle that picks a market ASKS and the next one places, which is
    // what `decide`'s settle guard refuses on by name.
    want.extend(candidate_keys(&target.market_id, &pm));
    // ...and, when a cross is possible, the two sides it would LIFT. Asked for
    // unconditionally under the flag rather than after pricing, because the
    // shape — and so which side a cross would hit — is not known until the
    // books are read, which is the same reason `candidate_keys` yields both.
    if live.take_ok {
        want.extend(cross_keys(&target.market_id, &pm));
    }
    request_suppress(want);
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
    let decided = decide(
        &mut live.cx,
        &live.fees,
        &records,
        &target,
        &pm,
        &quote,
        &view,
        Instant::now(),
        live.take_ok,
    );
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
    out.extend(place(live, order, &view, kalshi, pmus).await);
    out
}

/// CROSS BOTH LEGS. The first leg is sent marketable instead of rested; if it
/// fills, [`close_leg`] closes the other one exactly as it does after a resting
/// fill, re-priced against the book at that moment.
///
/// **THE FAILURE MODE IS THE ONE THIS MODULE ALREADY HAS.** Between the first
/// leg filling and the second one returning we are one-legged, which is true of
/// a resting exit too — the difference is only that we chose the moment. So
/// nothing new guards it: `close_leg` parks a shortfall, `heal` works it off,
/// and `maker_exit_unresolved` still ratchets.
///
/// A first leg that does NOT fill leaves the slot free and NOTHING resting,
/// which is the whole point of an IOC here: the alternative — a marketable GTC —
/// would rest at a price we chose for crossing, which is the one price we never
/// want to sit at.
async fn cross(
    live: &mut Live,
    order: Order,
    view: &EngineView,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    let plan = order.cross.clone().expect("cross() is only called with a cross plan");
    let (first_sink, close_sink) = sinks(order.shape, kalshi, pmus);
    let coid = client_order_id();
    let req = PlaceRequest {
        market: order.rest_market().to_string(),
        // The same side the rest would have taken — selling the YES we hold, or
        // bidding for the YES we are short. Only the PRICE and the TIF differ.
        side: match order.shape {
            Shape::RestKalshi => Side::Ask,
            Shape::RestPmUs => Side::Bid,
        },
        price: plan.limit.clone(),
        qty: order.qty,
        tif: Tif::Ioc,
        // NOT post-only, and that is the entire difference from `place`. A
        // post-only order priced to cross is REJECTED by the venue, which is
        // exactly what makes it safe to rest and exactly what makes it useless
        // here.
        post_only: false,
        client_order_id: coid.clone(),
    };
    let mut out = vec![format!(
        "[maker-exit] CROSS {} {}x {} @ {} [{}] locking {}/ct crossed (vs {}/ct rested, which is \
         the spread and both takers' fees) — id {coid}",
        match order.shape {
            Shape::RestKalshi => "SELL",
            Shape::RestPmUs => "BUY",
        },
        order.qty,
        order.rest_market(),
        plan.limit,
        order.shape.tag(),
        plan.lock_ct,
        order.lock_ct,
    )];
    let sk = first_sink.clone();
    let rq = req.clone();
    let oid = match tokio::task::spawn_blocking(move || sk.place(&rq)).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => {
            out.push(format!("[maker-exit] the cross was refused: {e}"));
            return out;
        }
        Err(e) => {
            out.push(format!("[maker-exit] cross task failed: {e}"));
            return out;
        }
    };
    PLACED.fetch_add(1, AtomicOrd::Relaxed);
    // Same reason as `place`: this never went through `Engine::dispatch`, so the
    // account-wide fill feed would otherwise see money nothing can explain.
    crate::engine::fill::note_sidecar_order(&oid);

    let mut filled = 0i64;
    let mut unreadable: Option<String> = None;
    for i in 0..crate::naked_act::FILL_POLLS {
        let sk = first_sink.clone();
        let id = oid.clone();
        match tokio::task::spawn_blocking(move || sk.filled_qty(&id)).await {
            Ok(Ok(n)) => {
                filled = n;
                unreadable = None;
                if filled >= 1 {
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
        // AN ACCEPTED IOC WE CANNOT READ IS NOT A NON-FILL. It may have taken
        // contracts. `resolve_vanished` owns exactly this shape of doubt for the
        // resting path; here the order is already terminal at the venue, so the
        // honest move is to say so loudly and let the positions reconciler find
        // it rather than to assume either way and act on the assumption.
        out.push(format!(
            "[maker-exit] the cross IOC {oid} (client {coid}) was ACCEPTED and its fill could \
             not be read: {e} — NOT closing the other leg, because closing against a fill we \
             cannot see is how one naked leg becomes two. `--positions-recon` reads venue truth \
             every cycle and will surface it."
        ));
        UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
        return out;
    }
    if filled < 1 {
        out.push(
            "[maker-exit] the cross IOC did not fill — the book moved between the decision and \
             the wire. Nothing rests and the slot is free; the candidate comes back round on \
             the next scan."
                .to_string(),
        );
        return out;
    }
    if filled < order.qty {
        out.push(format!(
            "[maker-exit] the cross filled {filled} of {} — the close below is sized to the \
             FILL, not the order, so the unfilled remainder was never opened and needs nothing.",
            order.qty
        ));
    }
    // From here it is indistinguishable from a resting fill, so it uses the same
    // code: `close_leg` prices the other leg against the book NOW, sends the
    // IOC, parks a shortfall and writes the `unwound` record.
    // `close_leg` takes the fill it must cover as an argument and clears
    // `live.resting` itself, so this never goes into `live` — there is nothing
    // resting to forget, which is the one thing a cross has that a rest does not.
    let r = Resting {
        order,
        venue_order_id: oid,
        client_order_id: coid,
        since: Instant::now(),
    };
    out.extend(close_leg(live, view, &r, filled, first_sink, close_sink).await);
    out
}

/// Rest ONE post-only GTC ask — or cross, when [`Order::cross`] says the spread
/// is worth paying.
async fn place(
    live: &mut Live,
    order: Order,
    view: &EngineView,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    if order.cross.is_some() {
        return cross(live, order, view, kalshi, pmus).await;
    }
    let (rest_sink, _) = sinks(order.shape, kalshi, pmus);
    let coid = client_order_id();
    let req = PlaceRequest {
        market: order.rest_market().to_string(),
        // SELLING the YES we hold, or BIDDING for the YES we are short. Both are
        // the passive side of the leg they act on.
        side: match order.shape {
            Shape::RestKalshi => Side::Ask,
            Shape::RestPmUs => Side::Bid,
        },
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
    let k = rest_sink.clone();
    let r = req.clone();
    match tokio::task::spawn_blocking(move || k.place(&r)).await {
        Ok(Ok(id)) => {
            PLACED.fetch_add(1, AtomicOrd::Relaxed);
            // This order never went through `Engine::dispatch`, so no ack ever
            // names it to the engine and its fill would arrive on the
            // account-wide feed as money nothing can explain. Tell the engine
            // the one thing that makes it explicable: the venue's own id.
            crate::engine::fill::note_sidecar_order(&id);
            let line = format!(
                "[maker-exit] RESTED {}x {} @ {} [{}] locking {}/ct — venue id {id}, client \
                 {coid}",
                order.qty,
                order.rest_market(),
                order.limit,
                order.shape.tag(),
                order.lock_ct
            );
            // THE HAND-OVER IS DISCHARGED BY THIS, and only by this. Something
            // other than the rotated-out market has the slot, which is the
            // whole thing the rotation was asking for.
            //
            // `served` is NOT cleared here: it is the lap, and a rest is one
            // market taking its turn WITHIN the lap rather than the end of it.
            // Clearing on every rest is what made this a two-name swap — the
            // set never grew past the incumbent, so the third candidate was
            // never reached. Only a completed lap clears it (`target`).
            live.handover_age = 0;
            live.resting = Some(Resting {
                order,
                venue_order_id: id,
                client_order_id: coid,
                since: Instant::now(),
            });
            vec![line]
        }
        Ok(Err(e)) if e.retry() == arb_venue::error::Retry::MarketHalted => {
            // PARKED BY THE KALSHI TICKER even when it is PM-US that halted,
            // because that ticker is the candidate identity `Live::target`
            // filters on — parking the PM slug would key the park on something
            // the selector never looks up, and the halted candidate would be
            // chosen again every cycle. The message names the market that
            // actually halted, which is not always the same string.
            let d = live.park(&order.market);
            vec![refuse(format!(
                "[maker-exit] {} is HALTED at the venue ({e}) — parking {} for {}s. No price, \
                 size or interval answers a halt; only the venue reopening does.",
                order.rest_market(),
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
            live.unaddressable.insert(order.market.clone());
            vec![format!(
                "[maker-exit] PLACE DID NOT COMPLETE on {} ({e}) — an ASK may be RESTING at \
                 the venue under an id this process never learned, so nothing here can cancel \
                 it and a fill on it would leave a naked PM-US short. Nothing else in this \
                 process will take on that market again either. CHECK BY HAND: \
                 client_order_id {coid}. maker_exit_unresolved is now {}.",
                order.market,
                unresolved()
            )]
        }
        Err(e) => {
            UNRESOLVED.fetch_add(1, AtomicOrd::Relaxed);
            live.unaddressable.insert(order.market.clone());
            vec![format!(
                "[maker-exit] PLACE TASK FAILED on {} ({e}) — same as above: the ask may or \
                 may not be resting. CHECK BY HAND: client_order_id {coid}",
                order.market
            )]
        }
    }
}

/// Is this fill-read failure the venue saying the order is GONE, rather than the
/// venue failing to answer?
///
/// The distinction is the whole of card #75. `filled_qty` refuses instead of
/// answering 0 when it cannot ask, and `manage` treated every refusal the same
/// way — hold, ask again next cycle. That is right for a timeout, a 503 or an
/// exhausted rate budget, where a later read gets a real answer. It is a trap for
/// a 404, where every later read returns the same 404 forever: the module then
/// polls a dead id once a cycle for as long as the process lives, and because
/// [`MAX_RESTING`] is 1 nothing else can ever rest. Live, that ran 3,531 times
/// over 58 h before anyone looked.
///
/// ONLY 404, and only past [`VANISHED_MIN_AGE_S`]. A 401 is a credential problem
/// and a 500 is the venue's; neither says anything about the order, and holding
/// is still right for both. `VenueError::Status`'s own doc note already names
/// this shape — "some statuses are success in disguise (a 404 on cancel means the
/// order is already gone)" — and `engine::cancel` has acted on it since quirk K4.
/// This applies the same reading on the read path.
fn is_vanished(e: &arb_venue::VenueError, age_s: f64) -> bool {
    matches!(e, arb_venue::VenueError::Status { status: 404, .. }) && age_s >= VANISHED_MIN_AGE_S
}

/// The order is gone from the venue. Did it SELL anything on its way out?
///
/// **THIS IS THE HALF THAT MAKES "FORGET IT" SAFE, AND IT MUST NOT BE SKIPPED.**
/// Gone has two causes with opposite consequences: the ask was cancelled or
/// expired unfilled (nothing happened, free the slot), or it filled and the venue
/// reaped the record (we are one leg short and the ledger still calls the basket
/// open). The 404 alone cannot tell them apart, and guessing "unfilled" is
/// precisely the failure the original hold was written to prevent — so this does
/// not guess. It asks a DIFFERENT source: venue truth for the account, the same
/// `net_positions` read `positions::read_net` reconciles against.
///
/// Expected is what the LEDGER says we still hold on this Kalshi market for this
/// relationship — every open lot's remaining quantity, corrections merged and
/// unwinds netted, via `naked_act::open_lots`. Compare with what the venue says:
///
///   * `venue >= expected` — nothing of ours sold. Forget the order, free the
///     slot, and say so.
///   * `venue < expected` — the shortfall is what traded. Close exactly that,
///     clamped to what the exit ordered, through the ordinary fill path.
///   * the read fails — we have learned nothing, so HOLD, exactly as before.
///     A `net_positions` that cannot answer is not evidence of no fill.
///   * the read is IMPOSSIBLE — a shortfall larger than the order, or a PM-US
///     map with no row for a slug the ledger holds — HOLD. That is the positions
///     endpoint dropping rows (`pmus-positions-partial-stale-sticky`); on
///     2026-09-05 it booked two phantom closes before these guards existed.
///     PM-US is read through `positions::pmus_consensus` for the same reason.
///
/// The comparison is `>=` and not `==` on purpose: another lot on the same ticker
/// from a relationship this exit does not name would make the venue count larger,
/// and only a SHORTFALL is evidence of a sale. An excess is somebody else's
/// business and is not this module's to interpret.
async fn resolve_vanished(
    live: &mut Live,
    r: &Resting,
    rest_sink: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> (Vec<String>, Option<i64>) {
    // The order that vanished is the RESTING one, so every read here addresses
    // its venue and its market. The sign convention follows the same rule
    // `close_shortfall` follows and for the same reason: `net_positions` is a
    // yes-count, so a Kalshi long is POSITIVE and a PM-US short is NEGATIVE,
    // and "how many did it transact" is `expected - held` in BOTH cases only
    // once `held` has been read with the right sign.
    let rest_venue = r.order.shape.rest_venue();
    let rest_market = r.order.rest_market().to_string();
    let records = match ledger::read(&live.ledger_path) {
        Ok(recs) => recs,
        Err(e) => {
            return (
                vec![format!(
                    "[maker-exit] {} has vanished from the venue (404) but the ledger is \
                     unreadable ({e}), so what we SHOULD hold there cannot be established — \
                     holding the slot rather than assuming it never filled",
                    r.order.rest_market()
                )],
                None,
            )
        }
    };
    let expected: i64 = crate::naked_act::open_lots(&records, &r.order.rel_id)
        .iter()
        .filter(|(_, _, rec)| {
            rec.get("legs")
                .and_then(|v| v.as_array())
                .is_some_and(|legs| {
                    legs.iter().any(|l| {
                        l.get("venue").and_then(|v| v.as_str()) == Some(rest_venue.as_str())
                            && l.get("market_id").and_then(|v| v.as_str())
                                == Some(rest_market.as_str())
                    })
                })
        })
        .map(|(_, qty, _)| *qty)
        .sum();
    // Venue truth, read the way the venue deserves. Kalshi's positions endpoint
    // answers whole, so one read is one answer. PM-US's does not: it serves
    // PARTIAL sets during incidents (`pmus-positions-partial-stale-sticky`), and a
    // slug it dropped reads as ZERO held — which this arithmetic would take for
    // "everything sold". On 2026-09-05 it did exactly that, twice in four
    // minutes: 51 held read as 0, and two 5-lot "fills" were booked that never
    // happened. So the PM-US read is the same consensus read `positions` acts on,
    // and the guards below refuse the shapes only a dropped row can produce.
    let read: Result<crate::positions::NetMap, String> = match r.order.shape {
        Shape::RestKalshi => {
            let k = rest_sink.clone();
            match tokio::task::spawn_blocking(move || k.net_positions()).await {
                Ok(Ok(m)) => Ok(m),
                Ok(Err(e)) => Err(e.to_string()),
                Err(e) => Err(format!("positions task failed: {e}")),
            }
        }
        Shape::RestPmUs => crate::positions::pmus_consensus(rest_sink).await,
    };
    let net = match read {
        Ok(m) => m,
        Err(e) => {
            return (
                vec![format!(
                    "[maker-exit] {} has vanished from the venue (404) and the positions read \
                     that would say whether it sold first also failed ({e}) — holding the slot. \
                     This is the one case that still stalls the recycler, and it stalls it \
                     SAFELY: no fill is being assumed away.",
                    r.order.rest_market()
                )],
                None,
            )
        }
    };
    // Expected-set completeness, the other half of that quirk's port requirement.
    // The ledger says we hold this slug; a map without the row is either a
    // position sold to exactly zero or a row the endpoint dropped, and from here
    // the two are indistinguishable. Refuse. A real sell-to-zero is a naked leg
    // recon confirms from its own consensus reads and `naked_act` closes; a
    // dropped row booked as a fill is money that never moved.
    if r.order.shape == Shape::RestPmUs && expected > 0 && !net.contains_key(&rest_market) {
        return (
            vec![format!(
                "[maker-exit] {} has vanished from the venue (404) and the ledger has {expected} \
                 open there, but the PM-US positions map has NO ROW for it — a DEGRADED read \
                 (partial set, see pmus-positions-partial-stale-sticky), not a sale. Holding \
                 the slot; recon owns any real discrepancy.",
                r.order.rest_market()
            )],
            None,
        );
    }
    let signed = net.get(&rest_market).copied().unwrap_or(0.0);
    let held = match r.order.shape {
        Shape::RestKalshi => signed,
        // A PM-US short YES is a NEGATIVE yes-count; the NO contracts we hold
        // are its magnitude. A position on the wrong side of zero is none of
        // ours, so it floors rather than counting backwards.
        Shape::RestPmUs => (-signed).max(0.0),
    };
    let sold = (expected as f64 - held).round() as i64;
    if sold <= 0 {
        live.resting = None;
        return (
            vec![format!(
                "[maker-exit] {} has vanished from the venue (404 on {}, after {:.0}s resting) \
                 and venue truth says it sold nothing — we hold {held} there and the ledger's \
                 open lots account for {expected}. Forgetting the order and freeing the slot; \
                 the candidate is re-decided from a fresh book next cycle.",
                r.order.rest_market(),
                r.venue_order_id,
                r.since.elapsed().as_secs_f64(),
            )],
            None,
        );
    }
    // A shortfall LARGER than the order is not something the order can explain.
    // Clamping it to the order size (as this once did) books a fill for the part
    // that fits and says nothing about the rest — the one thing a 51-versus-0
    // read must never become is a 5-lot close.
    if sold > r.order.qty {
        return (
            vec![format!(
                "[maker-exit] {} has vanished from the venue (404) and venue truth says we hold \
                 {held} there against {expected} open in the ledger — a shortfall of {sold}, MORE \
                 than the {} this exit ever ordered. This order cannot account for that; a \
                 positions read that dropped rows can. Holding the slot and booking nothing; \
                 recon owns any real discrepancy.",
                r.order.rest_market(),
                r.order.qty,
            )],
            None,
        );
    }
    (
        vec![format!(
            "[maker-exit] {} has vanished from the venue (404) and venue truth says it {} \
             first: we hold {held} there against {expected} open in the ledger, a shortfall \
             of {sold}. Crossing the other leg on {} for that many now.",
            r.order.rest_market(),
            // A resting ASK that goes means contracts SOLD; a resting BID that
            // goes means contracts BOUGHT. Same shortfall arithmetic, opposite
            // trade, and a reader reconciling this against the venue needs the
            // right verb.
            match r.order.shape {
                Shape::RestKalshi => "SOLD",
                Shape::RestPmUs => "BOUGHT",
            },
            r.order.close_market()
        )],
        Some(sold),
    )
}

/// Look after the resting ask: has it filled, and does it still pay?
async fn manage(
    live: &mut Live,
    view: &EngineView,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    let Some(r) = live.resting.clone() else { return Vec::new() };
    let (rest_sink, close_sink) = sinks(r.order.shape, kalshi, pmus);
    let k = rest_sink.clone();
    let id = r.venue_order_id.clone();
    let mut out: Vec<String> = Vec::new();
    // Set on the one path where the order is known to be OFF the venue already.
    // The close below must then neither cancel it nor re-read it: both address an
    // id the venue has just told us it does not have, and nothing more can trade
    // on an order that is not there.
    let mut vanished = false;
    let filled = match tokio::task::spawn_blocking(move || k.filled_qty(&id)).await {
        Ok(Ok(n)) => n,
        // GONE is not UNREADABLE. A 404 on an id the venue has been answering
        // about for cycles means the order is not there any more, and asking
        // again next cycle asks the same dead id forever — see [`is_vanished`].
        // Which of the two things "gone" means is settled against venue truth,
        // never assumed.
        Ok(Err(ref e)) if is_vanished(e, r.since.elapsed().as_secs_f64()) => {
            let (lines, sold) = resolve_vanished(live, &r, rest_sink).await;
            out.extend(lines);
            match sold {
                // It sold on the way out. Fall through to the ordinary close
                // path with the quantity venue truth established, so the PM-US
                // leg is closed and one `unwound` record books it.
                Some(n) => {
                    vanished = true;
                    n
                }
                // Either it sold nothing (slot freed inside `resolve_vanished`)
                // or we could not find out (slot held). Both are done here.
                None => return out,
            }
        }
        Ok(Err(e)) => {
            // NOT "it did not fill". `filled_qty` refuses rather than answering
            // 0 when it cannot ask, and treating that as unfilled is how a real
            // fill goes unclosed and unbooked.
            return vec![format!(
                "[maker-exit] could not read the fill state of {} ({e}) — leaving it resting \
                 and asking again next cycle",
                r.order.rest_market()
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
        // A VANISHED order needs neither step: it is already off the venue, so
        // there is no remainder to cancel and no further fill to race. Both calls
        // would address the id that just 404ed, and the re-read would 404 again
        // and fall to the lower bound we already have.
        let settled = if vanished {
            filled
        } else {
            out.push(format!(
                "[maker-exit] {} filled {filled} of {} — cancelling the remainder before \
                 closing, so nothing else can trade while the other leg is open",
                r.order.rest_market(), r.order.qty
            ));
            let (lines, unaddressable) = cancel_at_venue(rest_sink, &r).await;
            out.extend(lines);
            if let Some(m) = unaddressable {
                live.unaddressable.insert(m);
            }
            // ...and the count is RE-READ after the cancel. Anything that traded
            // between the poll and the cancel is ours too, and closing less than
            // we sold is precisely the naked leg this path exists to avoid.
            //
            // THROUGH THE REST SINK, which is the only venue that has ever heard
            // of this order id. This read was hardcoded to `kalshi` while that
            // was the only venue an exit could rest on; under `Shape::RestPmUs`
            // the id is a PM-US one, asking Kalshi about it errors, and the
            // error falls through to the lower bound below — so a fill that
            // raced the cancel would go naked and unbooked, which is the exact
            // failure this re-read exists to prevent.
            let k = rest_sink.clone();
            let id = r.venue_order_id.clone();
            match tokio::task::spawn_blocking(move || k.filled_qty(&id)).await {
                Ok(Ok(n)) => n.max(filled),
                // Unreadable after the cancel. `filled` is a LOWER bound and is
                // still the best number we have, so close that much rather than
                // nothing — and say that the difference, if any, is invisible.
                _ => {
                    out.push(format!(
                        "[maker-exit] could not re-read {} after the cancel — closing the \
                         {filled} we know traded. If the cancel raced a further fill, the \
                         difference is naked and unbooked; CHECK BY HAND.",
                        r.order.rest_market()
                    ));
                    filled
                }
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
        out.extend(close_leg(live, view, &r, settled.min(r.order.qty), rest_sink, close_sink).await);
        return out;
    }
    // Unfilled. Does the floor still sit at or below where we are resting? The
    // floor only moves with the ledger basis (fixed) and the PM close (not),
    // so a RISE means the PM book has moved against the exit and the price we
    // are resting at no longer locks anything.
    match still_pays(&mut live.cx, &live.fees, &r.order, view) {
        Ok(()) => out,
        Err(why) => {
            out.push(format!("[maker-exit] PULLING {} — {why}", r.order.market));
            out.extend(pull(live, kalshi, pmus).await);
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
    let (Some(k_basis), Some(pm_basis), Some(resting)) =
        (cx.parse(&o.k_basis), cx.parse(&o.pm_basis), cx.parse(&o.limit))
    else {
        return Err("a price on the resting exit does not parse".into());
    };
    let tick = cx.parse_exact(TICK);
    // A one-rung penny ladder is enough to re-derive the bound: we are only
    // asking whether it has passed the price we are already at, and the real
    // ladder was used to pick that price.
    let ladder = vec![("0.0000".to_string(), "1.0000".to_string(), "0.0100".to_string())];
    match o.shape {
        // We are RESTING an ask. The bound is a FLOOR and it rises as the PM-US
        // ask we would cross gets dearer.
        Shape::RestKalshi => {
            let Some(pm_ask) = view.pm_ask.get(&o.pm_market) else {
                return Err(format!(
                    "the PM-US book for {} has gone dark, so the close leg can no longer be \
                     valued",
                    o.pm_market
                ));
            };
            let Some(pm_ask_d) = cx.parse(pm_ask) else {
                return Err("a price on the resting exit does not parse".into());
            };
            let pm_close = cx.add(pm_ask_d, tick);
            let floor = exit_limit(
                cx, fees, &ladder, k_basis, pm_basis, pm_close, o.qty, Role::Maker, Role::Taker,
                EXIT_FLOOR,
            )?;
            if cx.cmp(resting, floor) == Ordering::Less {
                return Err(format!(
                    "the PM-US ask has moved to {pm_ask}, which puts the profit floor at {} — \
                     above the {} we are resting at, so a fill here would no longer lock \
                     {EXIT_FLOOR}/ct",
                    cx.emit_6dp(floor),
                    o.limit
                ));
            }
        }
        // We are RESTING a bid. The bound is a CEILING and it falls as the
        // Kalshi bid we would sell into gets cheaper — the same test with the
        // inequality turned over, for the same reason.
        Shape::RestPmUs => {
            let Some(k_bid) = view.k_bid.get(&o.market) else {
                return Err(format!(
                    "the Kalshi book for {} has gone dark, so the close leg can no longer be \
                     valued",
                    o.market
                ));
            };
            let Some(k_bid_d) = cx.parse(k_bid) else {
                return Err("a price on the resting exit does not parse".into());
            };
            let k_take = cx.sub(k_bid_d, tick);
            let ceiling = close_limit(
                cx, fees, k_take, k_basis, pm_basis, o.qty, Role::Taker, Role::Maker,
                EXIT_FLOOR,
            )?;
            if cx.cmp(resting, ceiling) == Ordering::Greater {
                return Err(format!(
                    "the Kalshi bid has moved to {k_bid}, which puts the profit ceiling at {} \
                     — below the {} we are bidding, so a fill here would no longer lock \
                     {MIN_LOCK}/ct",
                    cx.emit_6dp(ceiling),
                    o.limit
                ));
            }
        }
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
/// does — but the market joins [`Live::unaddressable`], so nothing else in this
/// process will cross it in the meantime either.
async fn pull(
    live: &mut Live,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    let Some(r) = live.resting.take() else { return Vec::new() };
    let (rest_sink, _) = sinks(r.order.shape, kalshi, pmus);
    let (out, unaddressable) = cancel_at_venue(rest_sink, &r).await;
    if let Some(m) = unaddressable {
        live.unaddressable.insert(m);
    }
    out
}

/// Send the cancel, WITHOUT touching [`Live::resting`].
///
/// Split from [`pull`] because the partial-fill path needs the cancel and must
/// NOT drop the record until it has re-read the fill count off the same id.
///
/// The second half of the answer is the market to stand everything else off,
/// when the cancel DID NOT COMPLETE and the ask may therefore still be resting.
/// It comes back as a value rather than being written here because both callers
/// already hold the `&mut Live`, and threading one in would give this function a
/// reason to touch the state it exists to leave alone.
async fn cancel_at_venue(
    rest_sink: &std::sync::Arc<dyn crate::sink::OrderSink>,
    r: &Resting,
) -> (Vec<String>, Option<String>) {
    use arb_venue::gateway::{CancelBy, CancelRequest};
    let k = rest_sink.clone();
    let req = CancelRequest {
        by: CancelBy::VenueId(r.venue_order_id.clone()),
        // PM-US requires the slug in the cancel body (quirk
        // `pmus-cancel-requires-market-slug`); Kalshi ignores it. Naming the
        // RESTING market is right for both.
        market_slug: Some(r.order.rest_market().to_string()),
    };
    match tokio::task::spawn_blocking(move || k.cancel(&req)).await {
        Ok(Ok(())) => (
            vec![format!(
                "[maker-exit] pulled {} ({}) after {:.0}s resting",
                r.order.rest_market(),
                r.venue_order_id,
                r.since.elapsed().as_secs_f64()
            )],
            None,
        ),
        Ok(Err(e)) => (
            vec![format!(
                "[maker-exit] CANCEL FAILED on {} ({e}) — order {} may still be RESTING. It \
                 carries client id {} and the account-wide sweep will reach it at kill or exit; \
                 nothing else will.",
                r.order.rest_market(), r.venue_order_id, r.client_order_id
            )],
            Some(r.order.rest_market().to_string()),
        ),
        // The cancel may never have been SENT, which is the same hazard as the
        // arm above wearing a different error: an ask that may still be resting.
        Err(e) => (
            vec![format!("[maker-exit] cancel task failed ({e})")],
            Some(r.order.rest_market().to_string()),
        ),
    }
}

/// The most a PM-US close may pay, checked against the ask the engine can see.
///
/// Split out of [`close_leg`] because it is the whole of `unwind` §5's fourth
/// bullet — "RE-PRICE AT FILL TIME ... AND ABANDON THE CLOSE IF IT NO LONGER
/// PAYS" — and a decision that lives inside an `async fn` that places orders is
/// a decision no test can reach.
/// Finish a close that did not complete, and clear the latch when it has.
///
/// Runs FIRST in [`cycle`], before `manage` and before anything is selected: a
/// naked leg is a real directional position and everything else here is
/// optional next to closing it.
///
/// It never assumes. Each cycle it re-reads venue truth, sizes the retry to the
/// shortfall ([`close_shortfall`]), re-prices against the current book, and
/// sends an IOC for exactly what is still owed. The latch clears only on
/// evidence — the shortfall reaching zero — never on a timer and never on a
/// count of attempts.
///
/// The price rule changes once, at [`HEAL_PROFITABLE_CYCLES`]: profitable-only
/// before it, and whatever the book asks after it. See that constant for why
/// the second half has to exist.
async fn heal(
    live: &mut Live,
    view: &EngineView,
    kalshi: &std::sync::Arc<dyn crate::sink::OrderSink>,
    pmus: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    let Some(mut p) = live.pending.take() else { return Vec::new() };
    p.attempts += 1;
    let mut out: Vec<String> = Vec::new();
    // The leg still open is the one the shape did NOT rest, and every venue
    // call below addresses it. Naming it once keeps the log lines honest about
    // which account is naked.
    let (_, close_sink) = sinks(p.order.shape, kalshi, pmus);
    let open_leg = p.order.close_market().to_string();
    let open_venue = match p.order.shape {
        Shape::RestKalshi => Venue::PolymarketUs,
        Shape::RestPmUs => Venue::Kalshi,
    };

    // 1. VENUE TRUTH FIRST. Without it there is no honest size for the retry,
    //    and a guess here is the naked-long failure `close_shortfall` exists to
    //    prevent. A read that cannot answer holds the state for the next cycle.
    let s = close_sink.clone();
    let net = match tokio::task::spawn_blocking(move || s.net_positions()).await {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => {
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — cannot read {} positions ({e}), so there is \
                 no honest size for the retry. Still naked, still latched; trying again next \
                 cycle (attempt {}).",
                open_venue.as_str(), p.attempts
            ));
            live.pending = Some(p);
            return out;
        }
        Err(e) => {
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — positions task failed ({e}); retrying next \
                 cycle"
            ));
            live.pending = Some(p);
            return out;
        }
    };
    let records = match ledger::read(&live.ledger_path) {
        Ok(r) => r,
        Err(e) => {
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — the ledger is unreadable ({e}); nothing may \
                 be sized off it. Still latched; retrying next cycle."
            ));
            live.pending = Some(p);
            return out;
        }
    };
    let owed = close_shortfall(&records, &p, net.get(&open_leg).copied().unwrap_or(0.0));

    // 2. NOTHING OWED IS A SUCCESS, NOT A NO-OP. The close completed — most
    //    likely the IOC whose fill we could not read — so the only thing missing
    //    is the ledger record, and writing it is what makes the books true again.
    if owed <= 0 {
        let ts = arb_core::clock::now_s();
        out.push(format!(
            "[maker-exit] HEALED {open_leg} — venue truth says the close completed after all \
             ({}x owed, 0 outstanding). Booking the unwind and clearing the latch.",
            p.filled
        ));
        // NO PRICE TO READ HERE, and that is the case this arm exists for: the
        // close completed by an IOC whose fill this process never got back, so
        // its id buys nothing. The limits are what is known.
        let rest = p.order.rest_limit().to_string();
        out.push(book(&live.ledger_path, &p.order, &rest, &rest, p.filled, ts));
        HEALED.fetch_add(1, AtomicOrd::Relaxed);
        return out;
    }

    // 3. PRICE IT. Profitable-only while the cheap window lasts.
    let forced = p.attempts > HEAL_PROFITABLE_CYCLES;
    let limit = match price_close(&mut live.cx, &live.fees, &p.order, owed, view) {
        Ok(l) => l,
        Err(why) if !forced => {
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — {owed} still owed and the close does not pay \
                 yet ({why}). Attempt {} of {HEAL_PROFITABLE_CYCLES} profitable-only; after \
                 that it crosses out regardless, because five naked contracts for months costs \
                 more than the spread does.",
                p.attempts
            ));
            live.pending = Some(p);
            return out;
        }
        Err(why) => {
            // Past the window. Take the book's price: the ask, one tick through,
            // so the IOC actually clears rather than reporting "unfilled" at a
            // limit nothing will meet.
            // The book we must cross is the OPEN leg's, and the direction is
            // the shape's: buy PM-US YES back through the ask, or sell the
            // Kalshi YES through the bid. One tick THROUGH either way, so the
            // IOC actually clears rather than reporting "unfilled" at a limit
            // nothing will meet.
            let touch = match p.order.shape {
                Shape::RestKalshi => view.pm_ask.get(&open_leg),
                Shape::RestPmUs => view.k_bid.get(&open_leg),
            };
            let Some(touch) = touch else {
                out.push(format!(
                    "[maker-exit] HEAL {open_leg} — {owed} still owed, past the \
                     profitable-only window, and the {} book has gone DARK ({why}). A close \
                     cannot be priced against no book at any policy. Still latched; retrying \
                     next cycle.",
                    open_venue.as_str()
                ));
                live.pending = Some(p);
                return out;
            };
            let Some(touch_d) = live.cx.parse(touch) else {
                out.push(format!(
                    "[maker-exit] HEAL {open_leg} — the touch {touch} does not parse; retrying"
                ));
                live.pending = Some(p);
                return out;
            };
            let tick = live.cx.parse_exact(TICK);
            let through = match p.order.shape {
                Shape::RestKalshi => live.cx.add(touch_d, tick),
                Shape::RestPmUs => live.cx.sub(touch_d, tick),
            };
            if !live.cx.is_pos(through) {
                out.push(format!(
                    "[maker-exit] HEAL {open_leg} — the touch is {touch} and one tick through \
                     it is not a price. Still latched; retrying next cycle."
                ));
                live.pending = Some(p);
                return out;
            }
            let through = live.cx.quantize_4dp(through);
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — CROSSING OUT. {owed} contract(s) still owed \
                 after {} attempts, and the close has not paid at any of them ({why}). Taking \
                 the book at {} to be FLAT: this realises a loss, and it is the cheaper side \
                 of the trade — the alternative is carrying {owed} naked contract(s) to \
                 resolution.",
                p.attempts,
                through.to_standard_notation_string()
            ));
            through.to_standard_notation_string()
        }
    };

    // 4. SEND IT, sized to the shortfall and never to `filled`.
    let coid = client_order_id();
    let req = PlaceRequest {
        market: open_leg.clone(),
        side: match p.order.shape {
            Shape::RestKalshi => Side::Bid,
            Shape::RestPmUs => Side::Ask,
        },
        price: limit.clone(),
        qty: owed,
        tif: Tif::Ioc,
        post_only: false,
        client_order_id: coid.clone(),
    };
    let s = close_sink.clone();
    let rq = req.clone();
    let oid = match tokio::task::spawn_blocking(move || s.place(&rq)).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => {
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — {} refused the retry ({e}); {owed} still \
                 owed, retrying next cycle (attempt {})",
                open_venue.as_str(), p.attempts
            ));
            live.pending = Some(p);
            return out;
        }
        Err(e) => {
            out.push(format!(
                "[maker-exit] HEAL {open_leg} — retry task failed ({e}); retrying next cycle"
            ));
            live.pending = Some(p);
            return out;
        }
    };
    crate::engine::fill::note_sidecar_order(&oid);

    // 5. WHATEVER IT FILLED, THE NEXT CYCLE RE-MEASURES. The poll here decides
    //    only whether to book NOW; an unreadable answer is not a failure any
    //    more, because `close_shortfall` will settle it against the venue in
    //    sixty seconds. That is the difference this whole path buys.
    let mut got = 0i64;
    for i in 0..crate::naked_act::FILL_POLLS {
        let s = close_sink.clone();
        let id = oid.clone();
        if let Ok(Ok(n)) = tokio::task::spawn_blocking(move || s.filled_qty(&id)).await {
            got = n;
            if got >= 1 {
                break;
            }
        }
        if i + 1 < crate::naked_act::FILL_POLLS {
            tokio::time::sleep(crate::naked_act::FILL_POLL_GAP).await;
        }
    }
    if got >= owed {
        let ts = arb_core::clock::now_s();
        out.push(format!(
            "[maker-exit] HEALED {open_leg} — the retry closed the last {owed} contract(s) at \
             {limit} after {} attempt(s) and {:.0}s naked. Booking the unwind and clearing the \
             latch.",
            p.attempts,
            p.since.elapsed().as_secs_f64()
        ));
        let close_px =
            filled_price_or_limit(
                &mut live.cx,
                close_sink,
                &oid,
                &limit,
                sells_yes(p.order.shape.close_venue()),
            )
                .await;
        out.push(book(&live.ledger_path, &p.order, p.order.rest_limit(), &close_px, p.filled, ts));
        HEALED.fetch_add(1, AtomicOrd::Relaxed);
        return out;
    }
    out.push(format!(
        "[maker-exit] HEAL {open_leg} — the retry filled {got} of {owed} at {limit}. Still \
         short; the next cycle re-measures against the venue and finishes it."
    ));
    live.pending = Some(p);
    out
}

/// How many PM-US YES a pending close still owes, measured at the VENUE.
///
/// **NEVER `filled`, AND THAT IS THE WHOLE POINT.** Two of the five ways a close
/// fails leave us genuinely unsure whether it traded — the place task failing,
/// and an accepted IOC whose fill could not be read. Retrying `filled` on either
/// of those buys the leg a second time and turns a naked short into a naked
/// LONG, which is a worse position arrived at by trying to be safe. So the retry
/// is sized the same way [`resolve_vanished`] sizes the Kalshi side: against a
/// shortfall the account can be asked about.
///
/// The arithmetic, on whichever leg the shape left open — the PM-US NO under
/// `rest-kalshi`, the Kalshi YES under `rest-pmus`:
///
/// ```text
///   L = contracts the ledger's open lots claim on the OPEN leg's market
///   V = contracts the venue says we actually hold on it
///   already_bought = L - V     (a completed close shows up here, however it
///                               completed, and whether or not we could read it)
///   still_owed     = filled - already_bought,  clamped to [0, filled]
/// ```
///
/// `still_owed == 0` means the close DID complete — the unreadable IOC filled
/// after all — and the only thing left to do is book it.
fn close_shortfall(records: &[Value], p: &PendingClose, venue_net: f64) -> i64 {
    // WHICH LEG IS STILL OPEN is the shape's answer, and the sign of "how many
    // do we hold" flips with it. `net_positions` reports a yes-count: PM-US NO
    // comes back NEGATIVE (`recon` prints `pmus tpoyc-2026-popleo -69`), a
    // Kalshi long YES comes back POSITIVE. Everything else about the
    // measurement is identical, which is why it is one function.
    let (venue, market) = match p.order.shape {
        Shape::RestKalshi => (Venue::PolymarketUs, p.order.pm_market.as_str()),
        Shape::RestPmUs => (Venue::Kalshi, p.order.market.as_str()),
    };
    let ledger_qty: i64 = crate::naked_act::open_lots(records, &p.order.rel_id)
        .iter()
        .filter(|(_, _, rec)| {
            rec.get("legs").and_then(|v| v.as_array()).is_some_and(|legs| {
                legs.iter().any(|l| {
                    l.get("venue").and_then(|v| v.as_str()) == Some(venue.as_str())
                        && l.get("market_id").and_then(|v| v.as_str()) == Some(market)
                })
            })
        })
        .map(|(_, qty, _)| *qty)
        .sum();
    // A position on the WRONG side of zero means there is nothing of ours left
    // to close on this market, so `held` floors at zero either way.
    let signed = match p.order.shape {
        Shape::RestKalshi => -venue_net,
        Shape::RestPmUs => venue_net,
    };
    let held = signed.round().max(0.0) as i64;
    let already = (ledger_qty - held).max(0);
    (p.filled - already).clamp(0, p.filled)
}

fn price_close(
    cx: &mut Cx,
    fees: &FeeSchedule,
    o: &Order,
    filled: i64,
    view: &EngineView,
) -> Result<String, String> {
    let fill = cx.parse(&o.limit).ok_or("the fill price does not parse")?;
    let k_basis = cx.parse(&o.k_basis).ok_or("the kalshi basis does not parse")?;
    let pm_basis = cx.parse(&o.pm_basis).ok_or("the pm basis does not parse")?;
    match o.shape {
        // The Kalshi ask filled at `fill`; solve for the dearest PM-US YES we
        // may buy back and still lock MIN_LOCK.
        Shape::RestKalshi => {
            let limit = close_limit(
                cx, fees, fill, k_basis, pm_basis, filled, Role::Maker, Role::Taker,
                MIN_LOCK,
            )?;
            let ask = view
                .pm_ask
                .get(&o.pm_market)
                .ok_or_else(|| format!("no PM-US ask for {} in the engine's book", o.pm_market))?;
            let ask_d = cx.parse(ask).ok_or("the pm ask does not parse")?;
            // A limit BELOW the ask cannot fill, and sending one is how a close
            // reports "unfilled" for a book that was never going to take it.
            // Say what is true.
            if cx.cmp(ask_d, limit) == Ordering::Greater {
                return Err(format!(
                    "the PM-US ask is {ask} and the most this close may pay is {} — the book \
                     has moved against the exit since the bid was rested",
                    cx.emit_6dp(limit)
                ));
            }
            // The limit, not the ask: we send at or above the touch, so the
            // worst fill is the limit, and the limit is the price the lock was
            // solved for.
            Ok(cx.quantize_4dp(limit).to_standard_notation_string())
        }
        // The PM-US bid filled at `fill`; solve for the cheapest Kalshi YES we
        // may sell at and still lock MIN_LOCK. Mirror image, and `exit_limit`
        // is the same solver run with the other leg pinned.
        Shape::RestPmUs => {
            let ladder = vec![("0.0000".to_string(), "1.0000".to_string(), "0.0100".to_string())];
            let limit = exit_limit(
                cx, fees, &ladder, k_basis, pm_basis, fill, filled, Role::Taker, Role::Maker,
                MIN_LOCK,
            )?;
            let bid = view
                .k_bid
                .get(&o.market)
                .ok_or_else(|| format!("no Kalshi bid for {} in the engine's book", o.market))?;
            let bid_d = cx.parse(bid).ok_or("the kalshi bid does not parse")?;
            // A sell limit ABOVE the bid cannot fill — the same statement as the
            // arm above with the inequality turned over.
            if cx.cmp(limit, bid_d) == Ordering::Greater {
                return Err(format!(
                    "the Kalshi bid is {bid} and the least this close may accept is {} — the \
                     book has moved against the exit since the bid was rested",
                    cx.emit_6dp(limit)
                ));
            }
            Ok(cx.quantize_4dp(limit).to_standard_notation_string())
        }
    }
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
    rest_sink: &std::sync::Arc<dyn crate::sink::OrderSink>,
    close_sink: &std::sync::Arc<dyn crate::sink::OrderSink>,
) -> Vec<String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    // The exit is spent either way: the contracts are gone from Kalshi. Forget
    // the resting order NOW so that no path below can leave it addressable and
    // no later cycle can poll it as if it were still working.
    live.resting = None;
    let mut out = vec![format!(
        "[maker-exit] FILLED {filled}x {} @ {} [{}] after {:.0}s resting — crossing to close \
         on {}",
        r.order.rest_market(),
        r.order.limit,
        r.order.shape.tag(),
        r.since.elapsed().as_secs_f64(),
        r.order.close_market()
    )];
    let limit = match price_close(&mut live.cx, &live.fees, &r.order, filled, view) {
        Ok(p) => p,
        Err(why) => {
            out.push(park(live, &r.order, filled, r.since, &why));
            return out;
        }
    };
    let coid = client_order_id();
    let req = PlaceRequest {
        market: r.order.close_market().to_string(),
        // BUYING the YES back is how a short YES is closed; SELLING it is how a
        // long one is. Which leg is left to close is exactly what the shape
        // says, and it is the opposite of the one that just filled.
        side: match r.order.shape {
            Shape::RestKalshi => Side::Bid,
            Shape::RestPmUs => Side::Ask,
        },
        price: limit.clone(),
        qty: filled,
        tif: Tif::Ioc,
        // Both halves set: the wire builders inline the TIF from `post_only`
        // and ignore `PlaceRequest::tif`, so the two must not be able to drift.
        post_only: false,
        client_order_id: coid.clone(),
    };
    let p = close_sink.clone();
    let rq = req.clone();
    let oid = match tokio::task::spawn_blocking(move || p.place(&rq)).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => {
            out.push(park(live, &r.order, filled, r.since, &format!("the close was refused: {e}")));
            return out;
        }
        Err(e) => {
            out.push(park(live, &r.order, filled, r.since, &format!("close task failed: {e}")));
            return out;
        }
    };
    // The PM-US half of the same problem the Kalshi place has: this IOC is not
    // the engine's, so its fill on the account-wide feed matches nothing there.
    crate::engine::fill::note_sidecar_order(&oid);
    let mut got = 0i64;
    let mut unreadable: Option<String> = None;
    for i in 0..crate::naked_act::FILL_POLLS {
        let p = close_sink.clone();
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
        out.push(park(
            live,
            &r.order,
            filled,
            r.since,
            &format!("the close IOC {oid} (client {coid}) was ACCEPTED and its fill could not be read: {e}"),
        ));
        return out;
    }
    if got < 1 {
        out.push(park(live, &r.order, filled, r.since, "the close IOC did not fill"));
        return out;
    }
    let booked = got.min(filled);
    if booked < filled {
        // Part of the exit closed. The remainder is a real naked short, and it
        // is alarmed for exactly what it is rather than folded into the book.
        out.push(park(
            live,
            &r.order,
            filled - booked,
            r.since,
            "the close IOC filled only partly",
        ));
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    // WHAT THE VENUES SAY, not what we told them to accept. Both legs are read:
    // the resting one filled at its own price only when it truly rested, and on
    // a cross it was an IOC like the other.
    let rest_px = filled_price_or_limit(
        &mut live.cx,
        rest_sink,
        &r.venue_order_id,
        r.order.rest_limit(),
        sells_yes(r.order.shape.rest_venue()),
    )
    .await;
    let close_px = filled_price_or_limit(
        &mut live.cx,
        close_sink,
        &oid,
        &limit,
        sells_yes(r.order.shape.close_venue()),
    )
    .await;
    // BOTH LEGS TOGETHER, against what a flattened basket can pay. Each price
    // has already cleared its own limit, but that check is one-sided and cannot
    // see an inverted read — see `plausible_pair`. A pair that fails falls back
    // to the limits, which is what this path booked before it could read fills
    // at all, and says so: the ledger is where every future exit's basis comes
    // from and a wrong price there is not one bad row.
    let (k_px, pm_px) = fills_by_venue(&r.order, &rest_px, &close_px);
    let (rest_px, close_px) = if plausible_pair(&mut live.cx, k_px, pm_px) {
        (rest_px.clone(), close_px.clone())
    } else {
        out.push(format!(
            "[maker-exit] REFUSED the venues' own fill prices for {}: kalshi {k_px} against \
             pm-us {pm_px} claims {} a contract out of a basket that can only pay $1.00. \
             Booking the limits instead — this is a price-convention fault, not a bad exit.",
            r.order.rel_id,
            "more than $1.50 or less than $0.50"
        ));
        (r.order.rest_limit().to_string(), limit.clone())
    };
    out.push(book(&live.ledger_path, &r.order, &rest_px, &close_px, booked, ts));
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
            // Not crossable by default: the cross is opt-in and every test written
            // before it keeps asserting the resting exit it was written about.
            cross_ct: f64::NEG_INFINITY,
            actionable: true,
            near_floor: false,
            resolves_estimated: true,
        }
    }

    /// [`decide`] asks `naked_act::inflight_check`, which is FAIL-CLOSED on a
    /// registry nobody has published — so every decide test has to clear it, the
    /// way that module's own tests do, under the guard that owns it.
    ///
    /// It clears the stand-off too: `naked_act::tests::allow_all` does the same,
    /// and the two registries are shared across the same set of tests.
    async fn allow_all() -> tokio::sync::MutexGuard<'static, ()> {
        let g = crate::naked_act::TEST_SERIAL.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        reset_standoff();
        g
    }

    /// A view with the market already yielded long enough to place.
    fn view(pm_ask: &str) -> EngineView {
        // A ONE-TICK PM-US BOOK BY DEFAULT, so `Shape::RestPmUs` has almost no
        // spread to capture and pays the Kalshi TAKER fee for the privilege:
        // shape A wins, and every test written before the shape contest keeps
        // asserting the exit it was written about. `wide_pm_view` is the
        // opposite case and the contest is pinned against both.
        let pm_bid = format!("{:.4}", pm_ask.parse::<f64>().unwrap_or(0.20) - 0.01);
        view_with(pm_ask, &pm_bid, "0.2000")
    }

    /// A view whose PM-US book is WIDE — the live shape of `KXFRENCHPRES-27-*`,
    /// where the mirror shape is worth several cents a contract.
    fn wide_pm_view(pm_ask: &str, pm_bid: &str, k_bid: &str) -> EngineView {
        view_with(pm_ask, pm_bid, k_bid)
    }

    fn view_with(pm_ask: &str, pm_bid: &str, k_bid: &str) -> EngineView {
        let then = Instant::now() - Duration::from_secs_f64(SUPPRESS_SETTLE_S + 1.0);
        EngineView {
            apr_bar: 16.0,
            global_cap_usd: 500.0,
            pm_ask: [("p-a".to_string(), pm_ask.to_string())].into_iter().collect(),
            pm_bid: [("p-a".to_string(), pm_bid.to_string())].into_iter().collect(),
            k_bid: [("K-a".to_string(), k_bid.to_string())].into_iter().collect(),
            // ALL FOUR sides yielded, which is what `cycle` publishes once
            // `--maker-exit-take` is on: it cannot know which shape will win
            // until it has priced both books, nor whether the winner will rest
            // or cross. The two REST sides are `candidate_keys`, the two LIFT
            // sides `cross_keys`.
            suppressed_at: [
                (("K-a".to_string(), SIDE_ASK.to_string()), then),
                (("p-a".to_string(), SIDE_BID.to_string()), then),
                (("K-a".to_string(), SIDE_BID.to_string()), then),
                (("p-a".to_string(), SIDE_ASK.to_string()), then),
            ]
            .into_iter()
            .collect(),
        }
    }

    // ---- the shape contest ------------------------------------------------

    /// THE FIX, AS A TEST. A wide PM-US book and a tight Kalshi one is the live
    /// shape of `KXFRENCHPRES-27-BRET` (Kalshi 3.1c median, PM-US 8.0c), and
    /// under the single old shape every exit rested on the TIGHT side: we
    /// captured 3c and paid 8c, on every contract, for as long as the module
    /// has existed.
    ///
    /// Gross of fees the two shapes differ by exactly
    /// `kalshi_spread - pmus_spread`, so this must now rest the BID on PM-US.
    #[tokio::test]
    async fn a_wide_pm_book_rests_the_bid_on_pmus_rather_than_the_ask_on_kalshi() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.78", "0.19")];
        // Kalshi 0.20/0.22 (2c), PM-US 0.16/0.24 (8c).
        let q = quote(Some("0.20"), Some("0.22"));
        let vw = wide_pm_view("0.24", "0.16", "0.20");
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), false)
            .expect("the mirror shape pays");
        assert_eq!(o.shape, Shape::RestPmUs, "the wider spread is PM-US's");
        assert_eq!(o.rest_market(), "p-a", "and that is where the order rests");
        assert_eq!(o.close_market(), "K-a", "the Kalshi leg is what gets crossed");
        // One tick inside the 0.16 bid, and strictly under the 0.24 ask.
        assert_eq!(o.limit, "0.1700", "one tick in front of the PM-US bid queue: {o:?}");
        assert!(
            o.runner_up_ct.is_some(),
            "shape A was priced too, and the log line must be able to show it: {o:?}"
        );
    }

    /// **THE CROSS IS OFF UNLESS ASKED FOR.** `--maker-exit-take` defaults false,
    /// and on a book where crossing would comfortably pay, `take_ok: false` must
    /// still plan a resting exit and nothing else.
    #[tokio::test]
    async fn without_the_flag_a_cross_is_never_planned() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        // The same basis the "clears" test below uses, so this proves the FLAG
        // and not the arithmetic: identical books, identical lot, cross planned
        // there and not here.
        let recs = vec![open_basket(1.0, 5, "0.57", "0.43")];
        let q = quote(Some("0.16"), Some("0.24"));
        let vw = wide_pm_view("0.22", "0.20", "0.16");
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), false)
            .expect("the resting shape pays");
        assert!(o.cross.is_none(), "the flag is off, so nothing crosses: {o:?}");
    }

    /// **CROSSING PAYS WHEN IT STILL CLEARS THE SELECTION FLOOR.**
    ///
    /// Resting was not slow, it was ineffective: 34 exits rested and none closed
    /// over the 27 h to 2026-08-24, because a 5-lot passive order joins a queue
    /// it cannot reach the front of — measured that day, ours sat behind 711
    /// lots on `popleo` and 6,551 on `dontru`.
    ///
    /// Kalshi 0.16/0.24 is the wide book, so shape A wins and the leg that would
    /// have rested is the Kalshi ask. Crossing it means SELLING into the bid, a
    /// tick under: 0.15.
    #[tokio::test]
    async fn a_cross_that_clears_the_selection_floor_is_planned() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        // Basis 0.434 + 0.430 = 0.864. Against 0.15 sold and 0.23 bought that
        // leaves ~0.035/ct after both takers' fees (~0.021 of the 0.056 gross) —
        // just OVER the floor. The refusal below moves ONLY the Kalshi basis, by
        // three cents, so the two bracket the boundary from either side.
        let recs = vec![open_basket(1.0, 5, "0.57", "0.43")];
        let q = quote(Some("0.16"), Some("0.24"));
        let vw = wide_pm_view("0.22", "0.20", "0.16");
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), true)
            .expect("the resting shape pays");
        assert_eq!(o.shape, Shape::RestKalshi, "the wider spread is Kalshi's");
        let c = o.cross.as_ref().expect("crossing clears the floor here: {o:?}");
        assert_eq!(c.limit, "0.1500", "a tick under the 0.16 bid we sell into: {o:?}");
        // The whole trade-off, asserted rather than described: crossing locks
        // strictly less than resting, and that difference is what buys certainty.
        let crossed: f64 = c.lock_ct.parse().expect("a decimal");
        let rested: f64 = o.lock_ct.parse().expect("a decimal");
        assert!(crossed < rested, "crossing pays the spread: {c:?} vs {}", o.lock_ct);
        assert!(crossed >= 0.02, "and still clears the 0.02 selection floor: {c:?}");
    }

    /// **A CROSS INTO OUR OWN QUOTE IS CANCELLED, NOT FILLED**, so the side it
    /// LIFTS has to be yielded exactly as the side it would rest on does.
    ///
    /// `self_trade_prevention_type: taker_at_cross` answers a taker that would
    /// cross our own resting order by cancelling THE TAKER. The entry quoter
    /// sits at the touch by design, and the touch is what a cross lifts — so
    /// without this the cross would come back unfilled and read as "the book
    /// moved", silently, on exactly the markets we quote. Ineffective rather
    /// than dangerous, which is why it needs a test rather than an alarm.
    ///
    /// Shape A wins here, so the lift side is the KALSHI BID; the view yields
    /// everything except that.
    #[tokio::test]
    async fn a_cross_whose_lift_side_is_not_yielded_rests_instead() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.57", "0.43")];
        let q = quote(Some("0.16"), Some("0.24"));
        let mut vw = wide_pm_view("0.22", "0.20", "0.16");
        vw.suppressed_at.remove(&("K-a".to_string(), SIDE_BID.to_string()));
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), true)
            .expect("the resting shape still pays");
        assert_eq!(o.shape, Shape::RestKalshi);
        assert!(
            o.cross.is_none(),
            "the bid we would sell into is still ours to cancel, so this rests: {o:?}"
        );
        assert_eq!(o.limit, "0.2300", "and rests where it always did: {o:?}");
    }

    /// ...AND IS REFUSED WHEN IT DOES NOT.    /// ...AND IS REFUSED WHEN IT DOES NOT. The same books against a basis that
    /// leaves only a thin lock: resting still pays, crossing does not, and the
    /// exit rests exactly as it always did.
    ///
    /// Against `MIN_EXIT_CT` (0.02) and not `MIN_LOCK` (0.005) on purpose:
    /// `unwind::consider` already refused this lot at anything under 0.02, so a
    /// cross that dropped below it would spend the spread to reach a trade the
    /// selector had rejected.
    #[tokio::test]
    async fn a_cross_that_falls_under_the_selection_floor_is_refused() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        // The other side of the bracket: only the Kalshi basis moves, 0.43 ->
        // 0.46. Resting still clears MIN_LOCK comfortably; crossing drops to
        // ~0.005/ct, under the 0.02 selection floor, and is refused.
        let recs = vec![open_basket(1.0, 5, "0.57", "0.46")];
        let q = quote(Some("0.16"), Some("0.24"));
        let vw = wide_pm_view("0.22", "0.20", "0.16");
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), true)
            .expect("the resting shape still pays");
        assert!(o.cross.is_none(), "crossing does not clear the floor, so it rests: {o:?}");
        assert_eq!(o.limit, "0.2300", "and rests where it always did: {o:?}");
    }

    /// The regression in the other direction, and the reason `view()` is tight
    /// by default: where Kalshi carries the wider spread the ORIGINAL shape is
    /// still right, and nothing about this change may quietly move those exits
    /// onto the other venue.
    #[tokio::test]
    async fn a_wide_kalshi_book_still_rests_the_ask_on_kalshi() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.78", "0.19")];
        // Kalshi 0.16/0.24 (8c), PM-US 0.20/0.22 (2c) — the mirror of the above.
        let q = quote(Some("0.16"), Some("0.24"));
        let vw = wide_pm_view("0.22", "0.20", "0.16");
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), false)
            .expect("the original shape pays");
        assert_eq!(o.shape, Shape::RestKalshi, "the wider spread is Kalshi's");
        assert_eq!(o.rest_market(), "K-a");
        assert_eq!(o.limit, "0.2300", "one tick inside the Kalshi offer: {o:?}");
    }

    /// A TIE GOES TO THE INCUMBENT, and it has to, because `marks::compute_row`
    /// breaks ties the same way. If the two disagreed the selector would admit a
    /// candidate on one shape's number and the placer would price the other —
    /// which is exactly the marker/placer split that #79 and #80 were both about.
    #[tokio::test]
    async fn an_exact_tie_goes_to_rest_kalshi_the_way_marks_breaks_it() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.78", "0.19")];
        // Identical spreads on both venues: the fee term is all that is left,
        // and it favours PM-US — so a TRUE tie needs the books symmetric AND is
        // still broken by fees. What this pins is that `decide` never prefers
        // the mirror without a strictly better number.
        let q = quote(Some("0.19"), Some("0.23"));
        let vw = wide_pm_view("0.23", "0.19", "0.19");
        let o = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), false)
            .expect("something pays");
        let a: f64 = o.lock_ct.parse().expect("lock parses");
        let b: f64 = o.runner_up_ct.as_ref().expect("both priced").parse().expect("parses");
        assert!(a >= b, "the chosen shape is never the worse one: {o:?}");
    }

    /// Neither shape pays: the refusal names BOTH, because "the book did not
    /// pay" now has two books in it and an operator reading one reason would be
    /// reading half the answer.
    #[tokio::test]
    async fn a_refusal_names_what_each_shape_would_have_done() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        // A lot whose basis (0.70 Kalshi + 0.70 PM NO) is far above anything a
        // cheap Kalshi book and a DEAR PM-US offer can return on either shape.
        let recs = vec![open_basket(1.0, 5, "0.30", "0.70")];
        let q = quote(Some("0.20"), Some("0.22"));
        let vw = wide_pm_view("0.90", "0.86", "0.20");
        let why = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), false)
            .expect_err("nothing pays");
        assert!(why.contains("rest-kalshi:"), "{why}");
        assert!(why.contains("rest-pmus:"), "{why}");
    }

    /// The suppression gate follows the SHAPE. A process that has yielded only
    /// the Kalshi ask may not rest a PM-US bid against a quote of its own that
    /// is still live — the collision the handshake exists to prevent, pointing
    /// at the other venue.
    #[tokio::test]
    async fn resting_on_pmus_needs_the_pmus_bid_yielded_not_the_kalshi_ask() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.78", "0.19")];
        let q = quote(Some("0.20"), Some("0.22"));
        let mut vw = wide_pm_view("0.24", "0.16", "0.20");
        vw.suppressed_at.remove(&("p-a".to_string(), SIDE_BID.to_string()));
        let why = decide(&mut cx, &fees, &recs, &cand(5, 1.0), "p-a", &q, &vw, Instant::now(), false)
            .expect_err("the bid side was never yielded");
        assert!(why.contains("p-a:bid"), "it names the side it needs: {why}");
    }

    // ---- the bid-side rest price ------------------------------------------

    /// One tick in front of the bid queue, the mirror of `rest_price`'s
    /// `ask - TICK`.
    #[test]
    fn a_resting_bid_fronts_the_bid_queue_when_the_spread_allows_it() {
        let (mut cx, _f) = ready();
        let ceiling = cx.parse_exact("0.9000");
        let p = rest_price_bid(&mut cx, ceiling, Some("0.16"), Some("0.24")).expect("priced");
        assert_eq!(cx.emit_6dp(p), "0.170000");
    }

    /// A one-tick book has no room in front, so the bid JOINS the touch rather
    /// than crossing it — post-only would reject a bid at the offer, which is
    /// `rest_price`'s hard floor with the inequality turned over.
    #[test]
    fn a_one_tick_book_joins_the_bid_rather_than_crossing_the_offer() {
        let (mut cx, _f) = ready();
        let ceiling = cx.parse_exact("0.9000");
        let p = rest_price_bid(&mut cx, ceiling, Some("0.20"), Some("0.21")).expect("priced");
        assert_eq!(cx.emit_6dp(p), "0.200000", "joined the bid, not lifted the offer");
    }

    /// The profit ceiling BINDS when it sits under the touch: the bid rests
    /// below the market and waits, which is the honest answer for a lot the
    /// book will not sell to us cheaply enough yet.
    #[test]
    fn a_ceiling_under_the_touch_rests_the_bid_below_the_market() {
        let (mut cx, _f) = ready();
        let ceiling = cx.parse_exact("0.1200");
        let p = rest_price_bid(&mut cx, ceiling, Some("0.16"), Some("0.24")).expect("priced");
        assert_eq!(cx.emit_6dp(p), "0.120000", "the ceiling wins, not the touch");
    }

    // ---- the close leg ----------------------------------------------------

    /// The ledger record's ROLES are the shape's, not a fixed property of the
    /// venue. Getting this wrong misattributes every fee the record accounts
    /// for — Kalshi maker is 0.0175 and Kalshi taker is 0.07, four times it.
    #[test]
    fn the_booked_roles_follow_the_shape_and_not_the_venue() {
        let mut o = resting_exit(5).order;
        o.shape = Shape::RestPmUs;
        let rec = close_record(&o, "0.20", "0.17", 5, 1.0);
        let legs = rec["legs"].as_array().expect("legs");
        assert_eq!(legs[0]["venue"], "kalshi");
        assert_eq!(legs[0]["role"], "taker", "rest-pmus crosses Kalshi: {rec}");
        assert_eq!(legs[1]["venue"], "polymarket_us");
        assert_eq!(legs[1]["role"], "maker", "rest-pmus rests on PM-US: {rec}");
        assert_eq!(rec["maker_exit_shape"], "rest-pmus");

        let a = close_record(&resting_exit(5).order, "0.20", "0.17", 5, 1.0);
        assert_eq!(a["legs"][0]["role"], "maker", "and the original shape is unchanged");
        assert_eq!(a["legs"][1]["role"], "taker");
    }

    /// **THE PRICES GO TO THE VENUES THAT PAID THEM.**
    ///
    /// The test above pins the ROLES and never looks at the numbers, which is
    /// exactly why the swap survived: callers hold the two fills as (the leg
    /// that RESTED, the leg CLOSED by IOC), and writing them straight onto
    /// (kalshi, pmus) is only right for one of the two shapes.
    ///
    /// Caught on 2026-08-25 against the log that placed it — the 08-23 exit
    /// rested a PM-US bid `@ 0.0200 on polymarket_us`, and its record carried
    /// 0.0200 on the KALSHI leg. `rest-pmus` is the shape the contest picks most
    /// often, so this was most of the unwound records in the ledger.
    #[test]
    fn a_rest_pmus_record_puts_each_price_on_the_venue_that_paid_it() {
        let mut o = resting_exit(5).order;
        o.shape = Shape::RestPmUs;
        // (rested = the PM-US bid @ 0.20, closed = the Kalshi IOC @ 0.17)
        let rec = close_record(&o, "0.20", "0.17", 5, 1.0);
        assert_eq!(rec["legs"][0]["venue"], "kalshi");
        assert_eq!(rec["legs"][0]["yes_price"], "0.17", "the Kalshi IOC's price: {rec}");
        assert_eq!(rec["legs"][1]["venue"], "polymarket_us");
        assert_eq!(rec["legs"][1]["yes_price"], "0.20", "the PM-US rest's price: {rec}");

        // `rest-kalshi` is the case where role order and venue order coincide.
        // It was always right and must stay byte-for-byte unchanged.
        let a = close_record(&resting_exit(5).order, "0.20", "0.17", 5, 1.0);
        assert_eq!(a["legs"][0]["yes_price"], "0.20", "kalshi rested at 0.20: {a}");
        assert_eq!(a["legs"][1]["yes_price"], "0.17", "pmus closed at 0.17: {a}");
    }

    /// A CROSS PAID TAKER ON BOTH LEGS, AND ITS RESTING LEG WAS NEVER RESTED.
    ///
    /// `Order::limit` is what a rest WOULD have used and on a cross was sent to
    /// nobody; `Cross::limit` is what the IOC was sent at. That substitution
    /// used to live inside `close_record` — it is [`Order::rest_limit`] now, so
    /// that `close_record` records what it is HANDED and callers can hand it a
    /// real fill price instead of any limit at all.
    #[test]
    fn a_crossed_record_books_both_takers_and_the_price_it_crossed_at() {
        let mut o = resting_exit(5).order;
        o.shape = Shape::RestPmUs;
        o.cross = Some(Cross { limit: "0.2200".into(), lock_ct: "0.025704".into() });
        assert_eq!(o.rest_limit(), "0.2200", "the IOC's price, not the rest's {}", o.limit);
        let rec = close_record(&o, o.rest_limit(), "0.17", 5, 1.0);
        assert_eq!(rec["legs"][0]["role"], "taker");
        assert_eq!(rec["legs"][1]["role"], "taker", "neither leg rested: {rec}");
        assert_eq!(
            rec["legs"][1]["yes_price"], "0.2200",
            "the price the IOC actually paid, not the rest's 0.20: {rec}"
        );
        assert_eq!(rec["maker_exit_lock_ct"], "0.025704", "the lock actually taken: {rec}");
        assert!(
            rec["note"].as_str().expect("a note").contains("CROSSED"),
            "and the note describes what happened: {rec}"
        );
    }

    /// ...and a leg that TRULY rested keeps its own price, because a post-only
    /// order fills at the price it rested at and nowhere else.
    #[test]
    fn an_uncrossed_rest_limit_is_the_orders_own() {
        let o = resting_exit(5).order;
        assert!(o.cross.is_none());
        assert_eq!(o.rest_limit(), o.limit);
    }

    // ---- what it actually got, not what it was told to accept -------------

    fn notional(qty: &str, taker: &str, complement: bool) -> OrderFill {
        OrderFill::Notional(arb_venue::resp::FilledCost {
            qty: qty.into(),
            taker_cost_usd: taker.into(),
            maker_cost_usd: "0".into(),
            taker_fees_usd: "0".into(),
            maker_fees_usd: "0".into(),
            complement,
        })
    }

    /// THE ROW THAT PAID FOR THIS CHANGE, verbatim from the account's own order
    /// history. A sell of 5 KXTIME-26-POP went out at a limit of 0.1200 and
    /// Kalshi filled it at the touch: `taker_fill_cost_dollars` 3.9208 on 5
    /// contracts is 0.7842 of NO, so 0.2158 of YES received — 9.6c BETTER than
    /// the limit. The ledger booked 0.1200.
    #[test]
    fn a_sell_is_read_as_the_complement_of_its_notional() {
        let mut cx = Cx::default();
        let f = notional("5.00", "3.920800", true);
        assert_eq!(fill_price(&mut cx, &f, "0.1200", true).as_deref(), Some("0.2158"));
    }

    /// ...and a BUY is the notional itself. Both directions are pinned because
    /// getting the orientation backwards inverts every recorded price, which is
    /// the same class of error as PR #97.
    #[test]
    fn a_buy_is_read_as_the_notional_itself() {
        let mut cx = Cx::default();
        let f = notional("5.00", "1.800000", false);
        assert_eq!(fill_price(&mut cx, &f, "0.3600", false).as_deref(), Some("0.3600"));
    }

    /// WHY `filled_price_or_limit` TAKES `selling` INSTEAD OF A VENUE.
    ///
    /// Inside this file the direction IS a property of the venue — every Kalshi
    /// leg of an exit sells its YES — and [`sells_yes`] says so. But
    /// `positions::place_and_book` shares the helper and goes BOTH ways on
    /// Kalshi: Case A BUYS the missing YES to complete a naked PM short.
    ///
    /// The numbers are the completion of 2026-09-01: an IOC to buy 5
    /// KXBTCMAXY-26DEC31-129999.99 at a limit of 0.1000, filled at the touch for
    /// 0.45 on 5 — a cent BETTER. Read as a buy it is booked. Read with
    /// `sells_yes(Kalshi)`, as a venue-derived direction would have it, "never
    /// worse than the limit" runs the wrong way, 0.0900 looks like a sell that
    /// received too little, and the caller silently keeps the limit — the exact
    /// behaviour this change exists to end, restored invisibly.
    #[test]
    fn the_recon_completions_buy_direction_is_not_the_venues() {
        let mut cx = Cx::default();
        let f = notional("5.00", "0.450000", false);
        assert_eq!(
            fill_price(&mut cx, &f, "0.1000", false).as_deref(),
            Some("0.0900"),
            "a BUY that filled a cent better than its limit"
        );
        assert!(sells_yes(Venue::Kalshi), "the exit's Kalshi leg really does sell");
        assert_eq!(
            fill_price(&mut cx, &f, "0.1000", true),
            None,
            "and that direction would refuse it straight back to the limit"
        );
    }

    /// A FILL MAY NEVER BE WORSE THAN THE LIMIT THAT AUTHORISED IT. A buy that
    /// claims to have paid MORE is not a bad fill, it is a conversion this file
    /// got wrong, and the ledger is where every future exit's basis is folded
    /// from. It refuses to the limit — exactly the old behaviour.
    #[test]
    fn a_price_worse_than_its_own_limit_is_refused() {
        let mut cx = Cx::default();
        let dear = notional("5.00", "3.920800", false); // 0.7842 paid...
        assert_eq!(fill_price(&mut cx, &dear, "0.3600", false), None, "...on a 0.36 limit");
        let cheap = OrderFill::Average { qty: 5, avg_px: "0.1000".into() };
        assert_eq!(fill_price(&mut cx, &cheap, "0.2000", true), None, "sold under a 0.20 limit");
    }

    /// THE GUARD IS ONE-SIDED AND THIS PINS THAT, so nobody reads the one above
    /// as total. Reading a SELL with the complement the wrong way round makes it
    /// look BETTER than its limit, not worse, so "never worse than the limit"
    /// cannot catch it: 3.9208 on 5 read as 0.7842 sails past a 0.1200 sell
    /// limit. [`plausible_pair`] is the check that does catch it, because it
    /// looks at what the two legs say TOGETHER.
    #[test]
    fn the_limit_guard_alone_cannot_catch_an_inverted_sell() {
        let mut cx = Cx::default();
        let inverted = notional("5.00", "3.920800", false);
        assert_eq!(fill_price(&mut cx, &inverted, "0.1200", true).as_deref(), Some("0.7842"));
        // ...and here is the thing that stops it reaching the ledger. Sold YES
        // at 0.7842 while buying the YES back at 0.20 claims $1.58 a contract
        // out of a pair that can only ever pay $1.00.
        assert!(!plausible_pair(&mut cx, "0.7842", "0.2000"));
        assert!(plausible_pair(&mut cx, "0.2158", "0.2000"), "the correct read is fine");
    }

    /// The pair bound is a BOUND, not a re-pricing: a flattened basket pays
    /// $1.00 a contract, so what the two legs together claim has to land near
    /// it. Real closes sit just under; nothing legitimate is near the edges.
    #[test]
    fn the_pair_bound_admits_real_closes_and_rejects_nonsense() {
        let mut cx = Cx::default();
        for (k, pm) in [("0.2158", "0.2000"), ("0.3600", "0.3300"), ("0.0300", "0.0600")] {
            assert!(plausible_pair(&mut cx, k, pm), "a real close: {k}/{pm}");
        }
        assert!(!plausible_pair(&mut cx, "0.9000", "0.0100"), "1.89/ct is not a flatten");
        assert!(!plausible_pair(&mut cx, "0.0100", "0.9000"), "0.11/ct is not one either");
    }

    /// PM-US answers an average price directly, already in the YES convention
    /// `pmus_order_body` sends. The guard applies to it identically.
    #[test]
    fn a_pmus_average_is_taken_as_given_and_still_guarded() {
        let mut cx = Cx::default();
        let better = OrderFill::Average { qty: 5, avg_px: "0.1300".into() };
        assert_eq!(fill_price(&mut cx, &better, "0.1500", false).as_deref(), Some("0.1300"));
        let worse = OrderFill::Average { qty: 5, avg_px: "0.1900".into() };
        assert_eq!(fill_price(&mut cx, &worse, "0.1500", false), None);
    }

    /// A price outside (0, 1) is not a probability and cannot be a fill. Zero
    /// especially: `avgPx=0-on-create` is a documented PM-US quirk, and booking
    /// it would record a leg that cost nothing.
    #[test]
    fn a_price_outside_the_unit_interval_is_not_a_fill() {
        let mut cx = Cx::default();
        for px in ["0", "1", "1.5", "-0.2"] {
            let f = OrderFill::Average { qty: 5, avg_px: px.into() };
            assert_eq!(fill_price(&mut cx, &f, "0.9999", false), None, "px {px}");
        }
        // ...and a zero quantity cannot be divided by.
        assert_eq!(fill_price(&mut cx, &notional("0", "1.0", false), "0.5", false), None);
    }

    /// The KALSHI leg of an exit always SELLS the YES and the PM-US leg always
    /// BUYS it back — there is no sell intent on the PM-US wire, so flattening a
    /// NO long there means buying the YES against it. The guard's direction is a
    /// property of the VENUE, not of which leg the shape rested.
    #[test]
    fn the_guards_direction_follows_the_venue_not_the_shape() {
        assert!(sells_yes(Venue::Kalshi));
        assert!(!sells_yes(Venue::PolymarketUs));
        for shape in [Shape::RestKalshi, Shape::RestPmUs] {
            assert_ne!(shape.rest_venue(), shape.close_venue(), "{shape:?} closes the OTHER leg");
            assert!(
                sells_yes(shape.rest_venue()) != sells_yes(shape.close_venue()),
                "one leg sells and the other buys, whichever rested: {shape:?}"
            );
        }
    }

    // ---- the naked leg, on whichever venue it is on -----------------------    // ---- the naked leg, on whichever venue it is on -----------------------

    /// THE SIGN FLIP, PINNED. `close_shortfall` measures the still-open leg
    /// against venue truth, and which leg that is — and therefore the sign of
    /// "how many do we hold" — is the shape's answer. `net_positions` reports a
    /// yes-count: a PM-US short YES comes back NEGATIVE, a Kalshi long YES
    /// POSITIVE. Reading one with the other's sign would size every retry at
    /// zero and clear the latch on a leg that is still naked, which is the
    /// worst failure this module can have.
    #[test]
    fn the_shortfall_reads_the_open_leg_with_that_venues_sign() {
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];

        // rest-kalshi: the Kalshi ask sold, the PM-US SHORT is what is left.
        // The ledger claims 5 NO; the venue still shows -5 (5 short), so
        // nothing has been bought back and all 5 are owed.
        let a = pending_for(5, 0);
        assert_eq!(close_shortfall(&recs, &a, -5.0), 5, "5 short at the venue, 5 owed");
        assert_eq!(close_shortfall(&recs, &a, 0.0), 0, "flat at the venue: the close landed");
        assert_eq!(close_shortfall(&recs, &a, -2.0), 2, "partly bought back");

        // rest-pmus: the PM-US bid filled, the Kalshi LONG is what is left. The
        // same three states, read with the opposite sign.
        let mut b = pending_for(5, 0);
        b.order.shape = Shape::RestPmUs;
        assert_eq!(close_shortfall(&recs, &b, 5.0), 5, "5 long at the venue, 5 still to sell");
        assert_eq!(close_shortfall(&recs, &b, 0.0), 0, "flat: the sell landed");
        assert_eq!(close_shortfall(&recs, &b, 2.0), 2, "partly sold");
        // ...and the WRONG sign must not read as "flat". This is the assertion
        // that would have failed had the mirror shape reused the PM arithmetic.
        assert_eq!(close_shortfall(&recs, &b, -5.0), 0, "a short Kalshi net is not ours to sell");
    }

    /// The profit test inverts with the shape: resting an ask has a FLOOR that
    /// rises as the close gets dearer, resting a bid has a CEILING that falls
    /// as the leg we sell into gets cheaper. Same test, inequality turned over.
    #[test]
    fn the_still_pays_bound_inverts_with_the_shape() {
        let (mut cx, fees) = ready();
        let mut o = resting_exit(5).order;
        o.shape = Shape::RestPmUs;
        // We are bidding 0.25 for the PM-US YES and will sell Kalshi into its
        // bid. A healthy Kalshi bid leaves the ceiling above our bid.
        o.limit = "0.2500".into();
        let mut vw = wide_pm_view("0.30", "0.24", "0.60");
        assert!(still_pays(&mut cx, &fees, &o, &vw).is_ok(), "the ceiling is above our bid");
        // The Kalshi bid collapses: there is far less coming back on that leg,
        // so the most we may pay for the PM YES falls under what we are bidding.
        vw.k_bid.insert("K-a".to_string(), "0.20".to_string());
        let why = still_pays(&mut cx, &fees, &o, &vw).expect_err("the ceiling has fallen");
        assert!(why.contains("ceiling"), "and it says which bound moved: {why}");
        assert!(why.contains("Kalshi bid"), "{why}");
    }

    /// The fill-time close crosses the OTHER venue, and under `rest-pmus` that
    /// is a Kalshi SELL priced off `exit_limit` — the same solver as the entry
    /// bound, run with the PM leg pinned to what it actually filled at.
    #[test]
    fn a_rest_pmus_close_prices_a_kalshi_sell_against_the_kalshi_bid() {
        let (mut cx, fees) = ready();
        let mut o = resting_exit(5).order;
        o.shape = Shape::RestPmUs;
        o.limit = "0.2500".into();
        let vw = wide_pm_view("0.30", "0.24", "0.60");
        let px = price_close(&mut cx, &fees, &o, 5, &vw).expect("priced");
        let px_f: f64 = px.parse().expect("parses");
        assert!(px_f <= 0.60, "at or under the bid, or the IOC cannot fill: {px}");
        assert!(px_f > 0.0, "and a real price: {px}");

        // A bid that has collapsed below what the lot needs is a refusal, not a
        // sale at any price: that is the abandon `unwind` §5 asks for.
        let mut dark = wide_pm_view("0.30", "0.24", "0.60");
        dark.k_bid.insert("K-a".to_string(), "0.02".to_string());
        let why = price_close(&mut cx, &fees, &o, 5, &dark).expect_err("does not pay");
        assert!(why.contains("Kalshi bid"), "{why}");
    }

    /// THE 3AM LINE. The naked alarm must name the side we are ACTUALLY naked
    /// on, and the two shapes leave opposite positions: `rest-kalshi` sells the
    /// Kalshi YES and leaves a PM-US SHORT uncovered, `rest-pmus` buys the
    /// PM-US YES back and leaves a Kalshi LONG uncovered. A message that named
    /// the wrong one would send a human to hedge the wrong way round.
    #[test]
    fn the_naked_alarm_names_the_side_the_shape_actually_left_open() {
        let a = alarm_unresolved(&resting_exit(5).order, 5, "the close was refused");
        assert!(a.contains("SOLD on Kalshi"), "{a}");
        assert!(a.contains("short 5 PM-US YES"), "{a}");
        assert!(a.contains("BUYING KALSHI BACK"), "{a}");

        let mut o = resting_exit(5).order;
        o.shape = Shape::RestPmUs;
        let b = alarm_unresolved(&o, 5, "the close was refused");
        assert!(b.contains("BOUGHT BACK on PM-US"), "{b}");
        assert!(b.contains("long 5 Kalshi YES"), "{b}");
        assert!(b.contains("SELLING PM-US SHORT"), "{b}");
        assert!(!b.contains("short 5 PM-US YES"), "the opposite claim must not survive: {b}");
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

        // Enough scans, not enough time. This needs a burst FASTER than
        // `CYCLE_S`, because `DEBOUNCE_S` is now exactly the time
        // `DEBOUNCE_SCANS` scans take at the normal cadence — so at 60 s apart
        // the two conditions land together and neither can be shown alone. A
        // burst is the case the clock still guards on its own: scans arriving
        // inside one window are not independent samples however many there are.
        let mut d = Debounce::default();
        for i in 0..8 {
            assert!(
                d.admit(&c, 1_000_000.0 + f64::from(i) * 10.0).is_empty(),
                "8 scans over 70s is under DEBOUNCE_S ({DEBOUNCE_S})"
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
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        let e = l.target(&c, t0 + 3.0 * DEBOUNCE_S, 0).expect("held long enough");
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
                shape: Shape::RestKalshi,
                lock_ct: "0.0100".into(),
                cross: None,
        runner_up_ct: None,
            },
            venue_order_id: "v1".into(),
            client_order_id: "x1".into(),
            since: Instant::now(),
        });
        let why = l.target(&c, t0 + 4.0 * DEBOUNCE_S, 0).expect_err("one at a time");
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
                shape: Shape::RestKalshi,
                lock_ct: "0.0100".into(),
                cross: None,
        runner_up_ct: None,
            },
            venue_order_id: "v1".into(),
            client_order_id: "x1".into(),
            since: Instant::now(),
        });
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        l.resting = None;
        let e = l.target(&c, t0 + 4.0 * DEBOUNCE_S, 0).expect("the clock kept running");
        assert_eq!(e.rel_id, "r1");
    }

    // ---- the latch --------------------------------------------------------

    /// A NAKED LEG WE MADE OURSELVES STOPS THE MODULE UNTIL A RESTART.
    ///
    /// `close_leg` books ONLY on full success and six of its arms alarm and
    /// return without a record, so the ledger goes on calling the lot open and
    /// `unwind` goes on selecting it. Without this, the next cycle rests ANOTHER
    /// ask against a quantity the venue has already sold — a clip of naked short
    /// per cycle, compounding, none of it visible to any exposure fold.
    #[test]
    fn nothing_new_rests_while_a_naked_leg_is_unresolved() {
        let mut l = Live::new(true, "/dev/null".into());
        let c = [cand(10, 1.0)];
        let t0 = 1_000_000.0;
        for i in 0..3 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        let before = refused();
        let why = l.target(&c, t0 + 3.0 * DEBOUNCE_S, 1).expect_err("one naked leg is enough");
        assert!(why.contains("1 naked leg(s) outstanding"), "it names the count: {why}");
        // The copy must NOT still promise a restart: `heal` clears this, and an
        // operator who reads "RESTARTED" at 3am restarts a process that was
        // already fixing itself — losing the debounce and the queue for nothing.
        assert!(!why.contains("RESTARTED"), "the restart claim is retracted: {why}");
        assert!(why.contains("`heal` IS WORKING ON IT"), "and says what clears it: {why}");
        assert_eq!(
            refused(),
            before,
            "a halt must not move the REFUSED gauge — 'the book did not pay' and 'we are naked' \
             are different facts and the module's own doc says they may never share a number"
        );
        // ...and the same candidate at the same instant, with nothing unresolved.
        let e = l.target(&c, t0 + 3.0 * DEBOUNCE_S, 0).expect("held long enough");
        assert_eq!(e.rel_id, "r1");
    }

    /// The debounce is folded on a HALTED cycle exactly as it is on a blocked
    /// one. The latch is about what may REST, not about what may be counted, and
    /// a halt that stopped the folding would restart every other candidate's
    /// clock on every pass.
    #[test]
    fn the_debounce_is_still_folded_while_halted() {
        let mut l = Live::new(true, "/dev/null".into());
        let c = [cand(10, 1.0)];
        let t0 = 1_000_000.0;
        for i in 0..3 {
            assert!(
                l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 1).is_err(),
                "halted, and still scanning"
            );
        }
        let e = l.target(&c, t0 + 3.0 * DEBOUNCE_S, 0).expect("the clock kept running");
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
            exit_limit(cx, &fees, &l, kb, pb, pc, 5, Role::Maker, Role::Taker, MIN_LOCK).map(|d| cx.emit_6dp(d))
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
        let e = exit_limit(&mut cx, &fees, &penny(), kb, pb, pc, 5, Role::Maker, Role::Taker, MIN_LOCK)
            .expect_err("a $1.15 lot whose PM leg is worth 5c cannot be recovered under $1");
        assert!(e.contains("at or above $1"), "{e}");
        assert!(e.contains("cannot be done"), "{e}");

        // ...and the mirror: when the PM leg alone more than repays the lot,
        // the floor is the smallest POSITIVE tick and never $0.00, which is not
        // a price but an offer to give the contracts away.
        let (kb, pb, pc) =
            (cx.parse_exact("0.05"), cx.parse_exact("0.10"), cx.parse_exact("0.01"));
        let l = exit_limit(&mut cx, &fees, &penny(), kb, pb, pc, 5, Role::Maker, Role::Taker, MIN_LOCK).expect("anything pays");
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
            let Ok(floor) = exit_limit(&mut cx, &fees, &l, k_basis, pm_basis, pm_close, 5, Role::Maker, Role::Taker, MIN_LOCK) else {
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
        let limit = exit_limit(&mut cx, &fees, &l, kb, pb, pc, 5, Role::Maker, Role::Taker, MIN_LOCK).expect("solvable");
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
        let p = close_limit(&mut cx, &fees, good, kb, pb, 5, Role::Maker, Role::Taker, MIN_LOCK).expect("a 0.25 fill closes");
        let p_s = cx.emit_6dp(p);

        // A BETTER Kalshi fill tolerates a DEARER close.
        let better = cx.parse_exact("0.30");
        let p2 = close_limit(&mut cx, &fees, better, kb, pb, 5, Role::Maker, Role::Taker, MIN_LOCK).expect("still closes");
        let p2_s = cx.emit_6dp(p2);
        assert!(p2_s > p_s, "a better fill buys more room: {p2_s} vs {p_s}");

        // A 97c basket can ALWAYS be closed at some PM price, because it cost
        // less than the dollar the pair pays out — a 2c Kalshi fill just means
        // the close has to be found under 4c. The first draft of this test
        // asserted an error there and was wrong about the arithmetic.
        let thin = cx.parse_exact("0.02");
        let p = close_limit(&mut cx, &fees, thin, kb, pb, 5, Role::Maker, Role::Taker, MIN_LOCK).expect("still closes, barely");
        let five = cx.parse_exact("0.05");
        assert!(cx.cmp(p, five) == Ordering::Less, "{}", cx.emit_6dp(p));

        // What has NO close is a lot that cost more than a dollar and whose
        // Kalshi leg came back small: no positive PM price leaves the lock.
        let (kb2, pb2) = (cx.parse_exact("0.30"), cx.parse_exact("0.85"));
        let e = close_limit(&mut cx, &fees, thin, kb2, pb2, 5, Role::Maker, Role::Taker, MIN_LOCK)
            .expect_err("$1.15 of basis against a 2c fill closes nothing");
        assert!(e.contains("no PM-US price"), "{e}");
    }

    // ---- the decision -----------------------------------------------------

    /// THE WHOLE DECISION, on the live ledger's shape.
    #[tokio::test]
    async fn a_held_candidate_with_a_ledger_basis_and_a_yielded_ask_prices_an_exit() {
        let _g = allow_all().await;
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
            false,
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
    #[tokio::test]
    async fn a_lot_our_own_ledger_cannot_vouch_for_is_never_exited() {
        let _g = allow_all().await;
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
            false,
        )
        .expect_err("no open lot");
        assert!(e.contains("no open ledger record for r1 opened at 1"), "{e}");
        assert!(e.contains("No other lot is substituted"), "{e}");

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
            false,
        )
        .expect_err("an inverted basket has no PM short to close");
        assert!(e.contains("PM-US leg"), "the refusal names the side: {e}");
    }

    /// THE ENTRY QUOTE COMES OFF FIRST, AND A BOUNDED WAIT FOLLOWS IT.
    /// `scripts/unwind_positions.py:45-49` — card ed6a5910, 14 soft unwinds
    /// deadlocked by self-trade prevention.
    #[tokio::test]
    async fn nothing_rests_until_the_entry_quoter_has_had_time_to_yield_the_ask() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let never = EngineView { suppressed_at: BTreeMap::new(), ..view("0.20") };
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &never, Instant::now(),
            false,
        )
        .expect_err("the quoter has not been told");
        assert!(e.contains("yield"), "{e}");

        let just_now = EngineView {
            suppressed_at: [(("K-a".to_string(), SIDE_ASK.to_string()), Instant::now())]
                .into_iter()
                .collect(),
            ..view("0.20")
        };
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &just_now, Instant::now(),
            false,
        )
        .expect_err("told, but not yet settled");
        assert!(e.contains("giving it"), "{e}");
    }

    /// THE EXIT IS PRICED OFF THE LOT `select` CHOSE, NOT THE DEAREST LEG OF
    /// EACH VENUE.
    ///
    /// The live shape, from `xvus-time-poty-26-artificialintelligence` on
    /// 2026-08-20: three open lots on one ticker, where the dearest KALSHI leg
    /// and the dearest PM-US leg are on DIFFERENT records. Kalshi fees ceil to
    /// the cent on the leg TOTAL, so the per-contract basis depends on the
    /// lot's size as well as its price:
    ///
    /// ```text
    ///   ts 1.0  5x  kalshi 0.10 maker -> 0.1020   pm 0.104 -> 0.896  = 0.9980
    ///   ts 2.0  5x  kalshi 0.10 taker -> 0.1080   pm 0.12  -> 0.880  = 0.9880
    ///   ts 3.0  4x  kalshi 0.10 taker -> 0.1075   pm 0.18  -> 0.820  = 0.9275  <- chosen
    /// ```
    ///
    /// `worst_lot` per leg composes the 0.1080 with the 0.896 — a basket nobody
    /// traded, and one dearer than the dollar the pair pays. It still produces
    /// a LEGAL price, which is what made this survivable enough to ship and
    /// invisible enough to run for a month: `exit_limit` answers 0.08, and this
    /// fixture reproduces the live number to the cent. Against a book offering
    /// 0.05 that ask is three rungs behind the entire queue. The lot actually
    /// selected wants 0.04. `maker_exit_closed` was 0 for the life of the
    /// deployment.
    #[tokio::test]
    async fn the_exit_is_priced_off_the_selected_lot_and_not_a_composite_of_the_dearest_legs() {
        // The function this no longer uses, imported HERE and nowhere else:
        // the test's job is to show what it would have priced.
        use crate::naked_act::worst_lot;
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let lot = |ts: f64, qty: i64, pm_yes: &str, k_role: &str| {
            v(&format!(
                r#"{{"ts":{ts},"relationship_id":"r1","status":"open","qty":{qty},"legs":[
                     {{"venue":"polymarket_us","market_id":"p-a","side":"no","role":"maker",
                       "qty":{qty},"yes_price":"{pm_yes}"}},
                     {{"venue":"kalshi","market_id":"K-a","side":"yes","role":"{k_role}",
                       "qty":{qty},"yes_price":"0.10"}}]}}"#
            ))
        };
        let recs = vec![
            lot(1.0, 5, "0.104", "maker"),
            lot(2.0, 5, "0.12", "taker"),
            lot(3.0, 4, "0.18", "taker"),
        ];

        // FIRST, the composite this used to price against is unexitable. Not a
        // hard exit — an IMPOSSIBLE one, which is the tell.
        let k = worst_lot(&mut cx, &fees, &recs, "r1", Venue::Kalshi, "K-a", Held::LongYes)
            .expect("a dearest kalshi leg exists");
        let pm = worst_lot(&mut cx, &fees, &recs, "r1", Venue::PolymarketUs, "p-a", Held::ShortYes)
            .expect("a dearest pm leg exists");
        assert_eq!((k.open_ts, pm.open_ts), (2.0, 1.0), "different records, which is the bug");
        let (kb, pb) = (cx.parse(&k.cost_per_ct).unwrap(), cx.parse(&pm.cost_per_ct).unwrap());
        let composite = cx.add(kb, pb);
        assert_eq!(
            cx.cmp(composite, cx.one),
            Ordering::Greater,
            "the composite is {} — the point is that it exceeds the $1 the pair pays, which \
             no real basket here does",
            cx.emit_6dp(composite)
        );
        let pm_close = cx.parse_exact("0.06");
        let composite_floor =
            exit_limit(&mut cx, &fees, &penny(), kb, pb, pm_close, 4, Role::Maker, Role::Taker, MIN_LOCK)
            .expect("legal, which is exactly why nobody caught it");
        assert_eq!(
            cx.emit_6dp(composite_floor),
            "0.080000",
            "the live mispricing, to the cent — an ask three rungs behind a 0.05 offer"
        );

        // NOW the real one. `cand(4, 3.0)` names the 0.9275 lot; against a
        // PM-US ask of 0.05 its floor is the bottom rung, and the Kalshi bid of
        // 0.03 lifts it to 0.04 — at the front of a book offering 0.05.
        let o = decide(
            &mut cx, &fees, &recs, &cand(4, 3.0), "p-a",
            &quote(Some("0.0300"), Some("0.0500")), &view("0.05"), Instant::now(),
            false,
        )
        .expect("the selected lot is exitable");
        assert_eq!(o.closes_ts, 3.0, "the record select named, not the dearest: {o:?}");
        assert_eq!(o.k_basis, "0.107500");
        assert_eq!(o.pm_basis, "0.820000");
        assert_eq!(o.qty, 4);
        assert_eq!(o.limit, "0.0400", "at the touch, not three ticks above the offer");
    }

    /// THE DEBOUNCE IS EXACTLY AS LONG AS ITS OWN SCAN COUNT TAKES.
    ///
    /// Both halves of [`Debounce::admit`] must bind together or one of them is
    /// decoration. `DEBOUNCE_S` below `(DEBOUNCE_SCANS - 1) * CYCLE_S` is never
    /// reached before the scans are, and above it we wait for no measured
    /// reason — the 900 s it used to be was compensating for a 120 s marks
    /// writer that was retired on 2026-07-31.
    ///
    /// Pinned as an identity so that moving `CYCLE_S` or `DEBOUNCE_SCANS` moves
    /// this with them instead of silently unbalancing the pair.
    #[test]
    fn the_debounce_is_the_time_its_scans_take() {
        assert_eq!(DEBOUNCE_S, (DEBOUNCE_SCANS as f64 - 1.0) * CYCLE_S as f64);
        assert_eq!(DEBOUNCE_S, 120.0, "two minutes, and the reason is in the constant's doc");
    }

    /// A SINGLE-SAMPLE SPIKE STILL CANNOT REACH A VENUE, which is the whole of
    /// what the debounce is for. One observation is not enough however long the
    /// clock says, and two is not enough either.
    #[test]
    fn one_sample_is_never_admitted_however_old_the_clock() {
        let mut d = Debounce::default();
        let e = [cand(5, 1.0)];
        let t0 = 1_000_000.0;
        // Seen once, then asked again an hour later: still one scan of a value
        // that may have existed for a single write.
        assert!(d.admit(&e, t0).is_empty(), "first sighting is never enough");
        assert!(d.admit(&e, t0 + 3600.0).is_empty(), "nor is the second, however stale");
        let out = d.admit(&e, t0 + 3600.0 + CYCLE_S as f64);
        assert_eq!(out.len(), 1, "three independent scans past the window admit it");
    }

    /// **OWNING AT 0.95 AND SELLING AT 0.96 IS NOT AN EXIT, IT IS A FEE.**    /// **OWNING AT 0.95 AND SELLING AT 0.96 IS NOT AN EXIT, IT IS A FEE.**
    ///
    /// The floor `decide` grades on used to be `MIN_LOCK` (0.005) while the
    /// selector that admitted the lot used `MIN_EXIT_CT` (0.02) — four times
    /// tighter. The gap was reachable, not theoretical: `consider` screens on a
    /// MARKS snapshot `select` will price off at up to 900 s old, and `decide`
    /// re-prices against the book NOW, so a lot admitted at 2c could go out at
    /// 0.6c on a book that had moved.
    ///
    /// This is that case with real numbers, and the answer is better than a
    /// refusal: THE SOLVER ASKS A HIGHER PRICE. The dear lot (PM leg cost 0.90,
    /// Kalshi 0.194) used to rest at 0.3300 for a lock of 0.012046/ct —
    /// comfortably over the old floor, comfortably under the one the selector
    /// used. Aiming the same solver at the same floor the selector used walks it
    /// one rung further up the ladder: 0.3400, locking 0.022046/ct.
    ///
    /// So the fix does not forgo the exit, it prices it. A refusal happens only
    /// when NO rung on the ladder reaches the floor — which is the honest answer
    /// in that case, because a price that does not exist cannot be rested at.
    #[tokio::test]
    async fn a_lock_between_the_two_floors_is_re_priced_not_taken() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 5, "0.10", "0.19")];
        let o = decide(
            &mut cx,
            &fees,
            &recs,
            &cand(5, 1.0),
            "p-a",
            &quote(Some("0.0100"), Some("0.1000")),
            &view("0.20"),
            Instant::now(),
            false,
        )
        .expect("the ladder has a rung that clears the selection floor");
        assert_eq!(o.limit, "0.3400", "one rung above the 0.3300 the old floor settled for: {o:?}");
        let lock: f64 = o.lock_ct.parse().expect("a decimal");
        assert!(
            lock >= 0.02,
            "and it clears the floor the selector admitted this lot under: {}",
            o.lock_ct
        );
    }

    /// The floor `decide` grades on IS the floor `unwind::consider` admits on.
    /// Two constants would drift, and the drift is invisible: the selector would
    /// go on admitting lots the wire then refuses, or worse, the other way.
    #[test]
    fn the_exit_floor_is_the_selection_floor() {
        assert_eq!(EXIT_FLOOR, crate::unwind::MIN_EXIT_CT_S);
    }

    /// EVERY LOT ON THE TICKER GETS ITS OWN PRICE.    /// EVERY LOT ON THE TICKER GETS ITS OWN PRICE.
    ///
    /// The corollary, and the reason a six-lot ticker empties in six passes
    /// rather than never: the dearer lots are dearer to exit, and they say so
    /// individually instead of all inheriting the worst one's floor.
    #[tokio::test]
    async fn each_lot_on_one_ticker_prices_to_its_own_basis() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        // Two lots differing ONLY in what the PM leg cost.
        let recs = vec![open_basket(1.0, 5, "0.10", "0.19"), open_basket(2.0, 5, "0.30", "0.19")];
        // A book whose touch (0.09) is under BOTH floors, so what is being
        // compared is the lots' own arithmetic and not the offer they share.
        let px = |cx: &mut Cx, ts: f64| {
            decide(
                cx, &fees, &recs, &cand(5, ts), "p-a",
                &quote(Some("0.0100"), Some("0.1000")), &view("0.20"), Instant::now(),
                false,
            )
            .map(|o| o.limit)
        };
        let dear = px(&mut cx, 1.0).expect("the 0.90 pm basis lot");
        let cheap = px(&mut cx, 2.0).expect("the 0.70 pm basis lot");
        assert_ne!(dear, cheap, "one floor for two different baskets is the bug");
        assert!(
            cx.parse(&dear).unwrap() > cx.parse(&cheap).unwrap(),
            "the lot that cost more needs more: dear {dear}, cheap {cheap}"
        );
    }

    /// A FLOOR AT OR UNDER THE BID MEANS THE BOOK IS PAYING MORE THAN THE LOT
    /// NEEDS, WHICH IS THE BEST CASE AN EXIT CAN BE HANDED — so it is priced
    /// off the BOOK (one rung inside the offer) rather than off the floor.
    ///
    /// This used to be `an_exit_that_would_cross_the_book_is_refused_rather_
    /// than_crossed`, and it asserted the refusal. The rule it pinned — "an ask
    /// at or under the bid is a take dressed as a maker order" — is true of the
    /// ORDER and says nothing about the SITUATION; see [`rest_price`]. The
    /// invariant survives in
    /// [`the_lift_clears_the_bid_strictly_so_post_only_cannot_reject_it`].
    #[tokio::test]
    async fn a_floor_under_the_bid_is_lifted_over_it_rather_than_walked_away_from() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        // Bid at 0.90 is far above any floor this basket produces: the book is
        // offering seventy cents more than the lot needs.
        let o = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.9000"), Some("0.9500")), &view("0.20"), Instant::now(),
            false,
        )
        .expect("a bid above the floor is the best case, not a refusal");
        assert_eq!(o.limit, "0.9400", "one rung inside the 0.95 offer, not the floor: {o:?}");
    }

    /// ...AND IT IS STILL A MAKER ORDER. The invariant the old refusal was
    /// protecting is the one that survives: post-only rejects an ask at or
    /// below the bid, so the price has to clear it STRICTLY. This pins the
    /// boundary the `!= Ordering::Greater` comparison used to guard, on the
    /// shape that tests it — a ONE-TICK spread, where "one rung inside the
    /// offer" IS the bid and only the lift saves it.
    #[test]
    fn the_lift_clears_the_bid_strictly_so_post_only_cannot_reject_it() {
        let (mut cx, _fees) = ready();
        let l = &penny();
        let floor = cx.parse_exact("0.0100");
        let p = rest_price(&mut cx, l, floor, Some("0.3000"), Some("0.3100")).expect("liftable");
        assert_eq!(cx.emit_6dp(p), "0.310000", "join the offer; an ask AT the bid is a take");
        // No ask to work inside, so the bid is the only thing to clear.
        let floor = cx.parse_exact("0.3000");
        let p = rest_price(&mut cx, l, floor, Some("0.3000"), None).expect("liftable");
        assert_eq!(cx.emit_6dp(p), "0.310000");
    }

    /// THE TOUCH IS ONE RUNG INSIDE THE OFFER, WHICH IS WHAT `marks` PROMISED.
    ///
    /// `marks::compute_row` prices `maker_exit_ct` at `k_ask - TICK`, and that
    /// is the number `unwind::consider` gates on. Resting at `bid + TICK`
    /// instead would select an exit on one price and ask another — 0.34
    /// promised against 0.33 asked on a `KXBRPRES-26-FBOL`-shaped book. It also
    /// gives away a spread we are under no obligation to give away.
    #[test]
    fn a_wide_book_is_undercut_at_the_offer_rather_than_dumped_on_the_bid() {
        let (mut cx, _fees) = ready();
        let floor = cx.parse_exact("0.1000");
        let p = rest_price(&mut cx, &penny(), floor, Some("0.3200"), Some("0.3500"))
            .expect("a three-tick book");
        assert_eq!(cx.emit_6dp(p), "0.340000", "inside the offer, not on top of the bid");
    }

    /// A FLOOR ABOVE THE TOUCH IS THE FLOOR, UNCHANGED.
    ///
    /// [`rest_price`] prices the book, it does not chase it below cost: where
    /// the book is not yet paying what the lot needs, the honest ask is the one
    /// the lot needs, even when that sits outside the market.
    #[test]
    fn a_floor_above_the_touch_is_left_exactly_where_it_is() {
        let (mut cx, _fees) = ready();
        let l = &penny();
        let floor = cx.parse_exact("0.0800");
        let p = rest_price(&mut cx, l, floor, Some("0.0300"), Some("0.0500")).expect("floor wins");
        assert_eq!(cx.emit_6dp(p), "0.080000");
        // ...and a market with NO sides at all is not a market priced at zero.
        let floor = cx.parse_exact("0.0800");
        let p = rest_price(&mut cx, l, floor, None, None).expect("no book, no touch");
        assert_eq!(cx.emit_6dp(p), "0.080000");
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
            false,
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
            false,
        )
        .expect_err("not ours");
        assert!(e.contains("--rel-prefix"), "{e}");
    }

    /// A DARK PM-US BOOK MAKES THE CLOSE UNPRICEABLE, SO THE EXIT IS
    /// UNPRICEABLE. Resting the Kalshi ask anyway would be committing to a
    /// two-leg trade having valued one leg.
    #[tokio::test]
    async fn an_exit_whose_close_leg_has_no_book_is_refused() {
        let _g = allow_all().await;
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let dark = EngineView { pm_ask: BTreeMap::new(), ..view("0.20") };
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &dark, Instant::now(),
            false,
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
            shape: Shape::RestKalshi,
            lock_ct: "0.0100".into(),
            cross: None,
        runner_up_ct: None,
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
    #[tokio::test]
    async fn a_filled_exit_whose_close_failed_alarms_on_its_own_gauge() {
        // Under the guard every other test that moves [`UNRESOLVED`] already
        // holds: the counter is process-global and never decrements, so an
        // unguarded `before + 1` here races the serialized bumpers and fails on
        // whichever side loses the interleaving. Caught live 2026-08-14: the
        // merge gate flaked on this test and on its guarded sibling on
        // consecutive runs, one each.
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
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
            shape: Shape::RestKalshi,
            lock_ct: "0.0100".into(),
            cross: None,
        runner_up_ct: None,
        };
        let before = unresolved();
        let refused_before = refused();
        let line = alarm_unresolved(&o, 3, "PM-US returned 503");
        assert_eq!(unresolved(), before + 1);
        assert_eq!(refused(), refused_before, "a naked leg is NOT a refusal");
        assert!(line.contains("NAKED AFTER EXIT"), "{line}");
        // It still names the fight with --positions-recon-act, because that
        // hazard is unchanged: recon-act completes a PmShort by buying Kalshi
        // back, which re-opens the basket this just exited.
        assert!(
            line.contains("BUYING KALSHI BACK"),
            "and it names the fight with --positions-recon-act: {line}"
        );
        // ...but it no longer sends anyone to do it by hand. The alarm's job is
        // now to say a heal is UNDER WAY and when to stop trusting it.
        assert!(!line.contains("RECONCILE BY HAND"), "the by-hand instruction is retracted: {line}");
        assert!(line.contains("`heal` HAS IT"), "{line}");
        assert!(line.contains("NO HAND NEEDED"), "{line}");
    }

    // ---- the fill path ----------------------------------------------------

    /// A venue that answers a scripted sequence and records the order of the
    /// calls, because the ORDER is the property under test: a cancel that
    /// happens after the close is not the same safety measure as one before it.
    #[derive(Default)]
    struct FakeVenue {
        /// `filled_qty` answers, consumed in order; the last one repeats.
        fills: Mutex<Vec<i64>>,
        /// What `place` answers instead of an id. A `Transport` error here is
        /// the request that never completed — the arm that cannot say whether
        /// an ask is resting.
        place_err: Mutex<Option<arb_venue::VenueError>>,
        /// What `filled_qty` answers INSTEAD of the scripted count. This is the
        /// arm the vanished-order path turns on: the venue refusing to answer,
        /// with a status that says why.
        fill_err: Mutex<Option<arb_venue::VenueError>>,
        /// What `net_positions` answers. `None` means the sink has no positions
        /// wired and refuses, which is itself one of the cases under test.
        net: Mutex<Option<BTreeMap<String, f64>>>,
        log: Mutex<Vec<String>>,
    }

    impl FakeVenue {
        fn with_fills(v: &[i64]) -> std::sync::Arc<FakeVenue> {
            std::sync::Arc::new(FakeVenue {
                fills: Mutex::new(v.to_vec()),
                place_err: Mutex::new(None),
                fill_err: Mutex::new(None),
                net: Mutex::new(None),
                log: Mutex::new(Vec::new()),
            })
        }
        /// The venue answers this error to every `filled_qty`.
        fn refusing(v: &std::sync::Arc<FakeVenue>, e: arb_venue::VenueError) {
            *v.fill_err.lock().unwrap() = Some(e);
        }
        /// ...and this is venue truth for the account.
        fn holding(v: &std::sync::Arc<FakeVenue>, market: &str, qty: f64) {
            *v.net.lock().unwrap() = Some([(market.to_string(), qty)].into_iter().collect());
        }
        fn status(code: u16) -> arb_venue::VenueError {
            arb_venue::VenueError::Status {
                endpoint: "kalshi order_status",
                status: code,
                body: r#"{"error":{"code":"not_found","message":"not found"}}"#.into(),
            }
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
            match self.place_err.lock().unwrap().clone() {
                Some(e) => Err(e),
                None => Ok("v1".into()),
            }
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
            if let Some(e) = self.fill_err.lock().unwrap().clone() {
                self.note(format!("filled_qty -> ERR {e}"));
                return Err(e);
            }
            let mut f = self.fills.lock().unwrap();
            let n = if f.len() > 1 { f.remove(0) } else { *f.first().unwrap_or(&0) };
            self.note(format!("filled_qty -> {n}"));
            Ok(n)
        }
        fn net_positions(
            &self,
        ) -> Result<BTreeMap<String, f64>, arb_venue::VenueError> {
            self.note("net_positions".into());
            match self.net.lock().unwrap().clone() {
                Some(m) => Ok(m),
                None => Err(arb_venue::VenueError::NotWired),
            }
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
                shape: Shape::RestKalshi,
                lock_ct: "0.0100".into(),
                cross: None,
        runner_up_ct: None,
            },
            venue_order_id: "v1".into(),
            client_order_id: "x1".into(),
            since: Instant::now(),
        }
    }

    /// The same exit, but rested long enough ago that a 404 on it cannot be the
    /// read-your-writes window.
    fn stale_resting_exit(qty: i64, age_s: f64) -> Resting {
        let mut r = resting_exit(qty);
        r.since = Instant::now() - Duration::from_secs_f64(age_s);
        r
    }

    /// A stale resting exit on a NAMED market, so a test can make each candidate
    /// the incumbent in turn. `resting_exit` is fixed to `K-a`, which is enough
    /// for the one-hand-over tests and not enough to walk a lap.
    fn stale_resting_exit_on(market: &str, qty: i64, age_s: f64) -> Resting {
        let mut r = stale_resting_exit(qty, age_s);
        r.order.market = market.into();
        r
    }

    /// Write one open basket to a scratch ledger so `resolve_vanished` has an
    /// expected quantity to compare venue truth against.
    fn ledger_with_open(tag: &str, qty: i64) -> String {
        let path = ledger_scratch(tag);
        std::fs::write(&path, format!("{}\n", open_basket(1.0, qty, "0.22", "0.19"))).unwrap();
        path
    }

    // ---- the vanished order -----------------------------------------------

    /// **A 404 IS THE VENUE ANSWERING, NOT THE VENUE FAILING TO ANSWER.**
    ///
    /// This is the wedge of card #75, as a test. `manage` treated every
    /// `filled_qty` refusal alike — hold, ask again next cycle — which is right
    /// for a timeout and a trap for a 404: every later read returns the same 404,
    /// so the module polls a dead id once a cycle for ever, and `MAX_RESTING` = 1
    /// means NOTHING ELSE CAN EVER REST. Live, that ran 3,531 times over 58 h
    /// while nine candidates were held behind it, and only a process restart
    /// could clear it.
    ///
    /// The fix must free the slot. It must NOT do so by assuming the ask went
    /// unfilled — that is the failure the original hold existed to prevent — so
    /// the venue's own position count is what settles it.
    #[tokio::test]
    async fn a_404_on_a_long_resting_exit_frees_the_slot_when_venue_truth_says_it_sold_nothing() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&kalshi, FakeVenue::status(404));
        // The ledger says 5 open on K-a; the venue says we hold all 5. Nothing
        // sold, so the vanished ask was cancelled or expired.
        FakeVenue::holding(&kalshi, "K-a", 5.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("vanished-clean", 5);
        let mut live = Live::new(false, path.clone());
        live.resting = Some(stale_resting_exit(5, VANISHED_MIN_AGE_S + 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_none(), "THE SLOT MUST BE FREED: {out}");
        assert!(out.contains("vanished"), "{out}");
        assert!(out.contains("sold nothing"), "and it says how it knows: {out}");
        assert!(
            kalshi.calls().iter().any(|c| c == "net_positions"),
            "the answer must come from venue truth, not from an assumption: {:?}",
            kalshi.calls()
        );
        // Nothing traded, so nothing is closed and nothing is booked.
        assert!(pmus.calls().is_empty(), "no close leg: {:?}", pmus.calls());
        assert!(crate::ledger::read(&path).unwrap().len() == 1, "no unwind record");
        let _ = std::fs::remove_file(&path);
    }

    /// ...AND IF IT SOLD ON ITS WAY OUT, THE PM-US LEG IS CLOSED FOR EXACTLY THE
    /// SHORTFALL.
    ///
    /// The dangerous half. A venue that reaps a FILLED order and then 404s its id
    /// looks identical, at the id, to one that cancelled an unfilled order. Guess
    /// "unfilled" here and the account is one leg short with the ledger still
    /// calling the basket hedged — which is precisely `maker_exit_unresolved`'s
    /// reason for existing. Venue truth tells the two apart: 5 open in the
    /// ledger, 2 held at Kalshi, so 3 traded.
    #[tokio::test]
    async fn a_404_after_a_fill_closes_the_shortfall_venue_truth_reveals() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[3]);
        FakeVenue::refusing(&kalshi, FakeVenue::status(404));
        FakeVenue::holding(&kalshi, "K-a", 2.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("vanished-sold", 5);
        let mut live = Live::new(false, path.clone());
        live.resting = Some(stale_resting_exit(5, VANISHED_MIN_AGE_S + 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(out.contains("SOLD"), "{out}");
        let ps = pmus.calls();
        assert!(ps[0].starts_with("place p-a 3x"), "closed the shortfall: {ps:?}");
        // ...and it is NOT cancelled or re-read: the order is already off the
        // venue, so both calls would address the id that just 404ed.
        assert!(
            !kalshi.calls().iter().any(|c| c == "cancel"),
            "nothing to cancel on an order the venue does not have: {:?}",
            kalshi.calls()
        );
        let recs = crate::ledger::read(&path).expect("clean ledger");
        assert_eq!(recs.len(), 2, "{recs:?}");
        assert_eq!(recs[1]["status"], "unwound");
        assert_eq!(recs[1]["qty"], 3, "the shortfall, not the whole ask");
        assert!(live.resting.is_none(), "a spent exit is not polled again");
        let _ = std::fs::remove_file(&path);
    }

    /// A 404 THE POSITIONS READ CANNOT CORROBORATE STILL HOLDS THE SLOT.
    ///
    /// The one case that still stalls the recycler, and it stalls it SAFELY. If
    /// `net_positions` cannot answer, we have learned nothing about whether the
    /// ask sold, and freeing the slot on no evidence is exactly the assumption
    /// this whole path refuses to make. Better a stalled recycler than a silent
    /// naked leg.
    #[tokio::test]
    async fn a_404_whose_positions_read_fails_holds_the_slot_rather_than_assuming() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&kalshi, FakeVenue::status(404));
        // `net` is left unset, so `net_positions` refuses.
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("vanished-blind", 5);
        let mut live = Live::new(false, path.clone());
        live.resting = Some(stale_resting_exit(5, VANISHED_MIN_AGE_S + 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_some(), "no evidence, no release: {out}");
        assert!(out.contains("holding the slot"), "{out}");
        assert!(out.contains("SAFELY"), "and says the stall is the safe side: {out}");
        assert!(pmus.calls().is_empty(), "nothing closed on a guess");
        let _ = std::fs::remove_file(&path);
    }

    /// A resting PM-US bid that has gone stale, for the mirror-shape 404 tests.
    fn stale_pmus_resting_exit(qty: i64, age_s: f64) -> Resting {
        let mut r = stale_resting_exit(qty, age_s);
        r.order.shape = Shape::RestPmUs;
        r
    }

    /// **A PM-US MAP WITH NO ROW FOR A SLUG THE LEDGER HOLDS IS A DEGRADED READ,
    /// NOT A SALE.**
    ///
    /// The 2026-09-05 incident, as a test. A rest-pmus exit for 5 of the 51 NO
    /// contracts the ledger held on one slug 404ed; the positions endpoint
    /// answered WITHOUT that slug (`pmus-positions-partial-stale-sticky`), a
    /// missing row read as 0 held, "51 - 0" was clamped to the order's 5, and a
    /// 5-lot close was booked and its Kalshi leg sold — twice, four minutes
    /// apart. Recon then found the account long 10 on PM-US against the ledger
    /// and bought them back. The dashboard shows two profitable round trips that
    /// never happened.
    ///
    /// The quirk's port requirement is consensus AND expected-set completeness:
    /// the slug we hold has to be IN the map before its count means anything.
    #[tokio::test(start_paused = true)]
    async fn a_404_on_a_pmus_exit_whose_positions_map_lacks_the_slug_holds_the_slot() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&pmus, FakeVenue::status(404));
        // Stable across reads (so consensus passes) and missing our slug.
        FakeVenue::holding(&pmus, "p-other", -3.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("vanished-pmus-norow", 51);
        let mut live = Live::new(false, path.clone());
        live.resting = Some(stale_pmus_resting_exit(5, VANISHED_MIN_AGE_S + 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_some(), "a dropped row is not evidence of a sale: {out}");
        assert!(out.contains("NO ROW"), "{out}");
        assert!(out.contains("DEGRADED"), "and names the quirk, not a fill: {out}");
        assert!(kalshi.calls().is_empty(), "NOTHING crossed on Kalshi: {:?}", kalshi.calls());
        assert_eq!(crate::ledger::read(&path).unwrap().len(), 1, "no phantom unwind record");
        let _ = std::fs::remove_file(&path);
    }

    /// ...AND A SHORTFALL THE ORDER CANNOT EXPLAIN IS REFUSED ON EITHER VENUE.
    ///
    /// The second guard, independent of the first: 51 open, 40 held, so 11 gone
    /// — but this exit only ever asked for 5. Clamping to 5 (what this once did)
    /// books a fill for the part that fits and says nothing about the other 6.
    /// Whatever moved 11 contracts, it was not a 5-lot order, so the path books
    /// nothing and leaves the discrepancy to recon, which reads for itself.
    #[tokio::test]
    async fn a_404_whose_shortfall_exceeds_the_order_holds_the_slot() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&kalshi, FakeVenue::status(404));
        FakeVenue::holding(&kalshi, "K-a", 40.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("vanished-too-short", 51);
        let mut live = Live::new(false, path.clone());
        live.resting = Some(stale_resting_exit(5, VANISHED_MIN_AGE_S + 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_some(), "11 gone is not this order's doing: {out}");
        assert!(out.contains("MORE than the 5"), "{out}");
        assert!(pmus.calls().is_empty(), "no close leg: {:?}", pmus.calls());
        assert_eq!(crate::ledger::read(&path).unwrap().len(), 1, "nothing booked");
        let _ = std::fs::remove_file(&path);
    }

    /// PM-US POSITIONS ARE READ BY CONSENSUS HERE, AS EVERYWHERE ELSE THAT ACTS.
    ///
    /// One raw read was what this path did before; `positions::pmus_consensus` is
    /// what recon and the hedger act on, and the same witness deserves the same
    /// scepticism at every call. Two agreeing reads, then the ordinary verdict.
    #[tokio::test(start_paused = true)]
    async fn a_404_on_a_pmus_exit_reads_positions_by_consensus() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&pmus, FakeVenue::status(404));
        // We hold all 5 NO (a -5 yes-count): the bid went unfilled.
        FakeVenue::holding(&pmus, "p-a", -5.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("vanished-pmus-clean", 5);
        let mut live = Live::new(false, path.clone());
        live.resting = Some(stale_pmus_resting_exit(5, VANISHED_MIN_AGE_S + 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_none(), "sold nothing, slot freed: {out}");
        assert!(out.contains("sold nothing"), "{out}");
        let reads = pmus.calls().iter().filter(|c| *c == "net_positions").count();
        assert_eq!(reads, 2, "two agreeing reads, not one: {:?}", pmus.calls());
        assert!(kalshi.calls().is_empty(), "no close leg: {:?}", kalshi.calls());
        let _ = std::fs::remove_file(&path);
    }

    /// A 404 INSIDE THE READ-YOUR-WRITES WINDOW IS STILL "NOT YET", NOT "GONE".
    ///
    /// Neither venue's create is read-your-writes — Kalshi 404s a GET on an order
    /// it has just accepted — which is why `filled_qty` polls through
    /// `Settle::retry_404` first. A blanket "404 means gone" would forget an ask
    /// that IS resting and then rest a second one on the same market, which is
    /// the duplicate `MAX_RESTING` exists to forbid. Age is what separates them.
    #[tokio::test]
    async fn a_404_on_a_freshly_rested_exit_is_held_not_forgotten() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&kalshi, FakeVenue::status(404));
        FakeVenue::holding(&kalshi, "K-a", 5.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let mut live = Live::new(false, ledger_scratch("vanished-young"));
        live.resting = Some(stale_resting_exit(5, VANISHED_MIN_AGE_S - 1.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_some(), "too young to call gone: {out}");
        assert!(out.contains("could not read the fill state"), "{out}");
        assert!(
            !kalshi.calls().iter().any(|c| c == "net_positions"),
            "and it does not spend a positions read to find that out: {:?}",
            kalshi.calls()
        );
    }

    /// ONLY 404. A 503 IS THE VENUE FAILING TO ANSWER AND STILL HOLDS.
    ///
    /// The classification must stay narrow. A 500, a timeout or an exhausted rate
    /// budget say nothing whatever about the order, and a later read gets a real
    /// answer — so the original hold is right for all of them, however long the
    /// ask has been resting.
    #[tokio::test]
    async fn a_non_404_refusal_holds_however_old_the_exit_is() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let kalshi = FakeVenue::with_fills(&[0]);
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::refusing(&kalshi, FakeVenue::status(503));
        FakeVenue::holding(&kalshi, "K-a", 5.0);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let mut live = Live::new(false, ledger_scratch("vanished-503"));
        live.resting = Some(stale_resting_exit(5, VANISHED_MIN_AGE_S * 100.0));
        let out = manage(&mut live, &view("0.20"), &k, &p).await.join("\n");

        assert!(live.resting.is_some(), "a 503 is not evidence the order is gone: {out}");
        assert!(out.contains("could not read the fill state"), "{out}");
    }

    // ---- the contested slot -----------------------------------------------

    /// **AN ASK NOBODY LIFTS MUST NOT OWN THE ONLY SLOT FOR EVER.**
    ///
    /// The deeper half of the same incident, and it needed no venue error at all:
    /// the `KXTIME-26-AI` ask rested for 2.3 DAYS without filling, and because
    /// `MAX_RESTING` is 1 the recycler was shut the whole time. `still_pays`
    /// pulls an ask the PM book has moved away from; nothing pulled one that was
    /// simply never going to trade. The slot is the scarce resource and nothing
    /// rationed it.
    #[test]
    fn a_stale_ask_gives_up_the_slot_when_other_candidates_are_waiting() {
        let mut l = Live::new(true, "/dev/null".into());
        // Two candidates on DIFFERENT markets, both held past the debounce.
        let mut other = cand(10, 2.0);
        other.market_id = "K-b".into();
        let c = [cand(10, 1.0), other];
        let t0 = 1_000_000.0;
        l.resting = Some(stale_resting_exit(5, MAX_RESTING_S + 1.0));
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        let reason = l.rotate.take().expect("the slot has been held too long");
        assert!(reason.contains("held the only exit slot"), "{reason}");
        assert!(reason.contains("so one of them can have it"), "{reason}");

        // AND THE SLOT ACTUALLY CHANGES HANDS. Rotating and then handing the
        // slot straight back is the pure loss `MAX_RESTING_S` says it will not
        // pay: on 2026-08-20 `KXTIME-26-AI` was rotated out at 11:46 and rested
        // again, same price, at 11:50, because `target` takes the first
        // admitted candidate and the incumbent was still first.
        l.resting = None;
        let next = l.target(&c, t0 + 4.0 * DEBOUNCE_S, 0).expect("somebody takes the slot");
        assert_eq!(next.market_id, "K-b", "the rotated-out market must not win it straight back");

        // ...and it KEEPS it across the cycles that cannot place.
        //
        // RETRACTED. This used to read `assert_eq!(after.market_id, "K-a", "the
        // courtesy is spent; normal ordering resumes")` — one selection and the
        // incumbent was back. That pinned the bug rather than the behaviour: the
        // selection above is the one that ASKS for the entry quote to be
        // yielded, `decide` refuses it for exactly that reason, and so the
        // courtesy was always spent without anything having rested. `K-a` then
        // took the slot straight back, which is the loss the two comments above
        // both say this must not pay. See [`HANDOVER_CYCLES`] and
        // [`the_hand_over_survives_the_cycles_that_cannot_place`].
        let after = l.target(&c, t0 + 5.0 * DEBOUNCE_S, 0).expect("still a candidate");
        assert_eq!(after.market_id, "K-b", "nothing rested, so the claim still stands");
    }

    /// THE HAND-OVER MUST SURVIVE THE REFUSAL THAT ALWAYS FOLLOWS IT.
    ///
    /// `rotated_out` used to be consumed by the selection that read it. That can
    /// never work, because the cycle which first picks a market is the cycle
    /// that ASKS for its entry quote to be yielded, and `decide` refuses
    /// everything whose quote has not been yielded yet. So the courtesy was
    /// always spent on a guaranteed refusal and the incumbent — still first in
    /// `select`'s order — took the slot straight back.
    ///
    /// This replays the live sequence of 2026-08-23, where `KXNOBELPEACE-26-DJT`
    /// held the only slot against 20 other eligible rows worth about $144:
    /// rotate, then TWO selections in which nothing manages to rest, and the
    /// challenger must still hold the claim on the second one.
    #[test]
    fn the_hand_over_survives_the_cycles_that_cannot_place() {
        let mut l = Live::new(true, "/dev/null".into());
        let mut other = cand(10, 2.0);
        other.market_id = "K-b".into();
        // `K-a` sorts first, which is why it was the incumbent and why it would
        // reclaim the slot the moment the hand-over lapsed.
        let c = [cand(10, 1.0), other];
        let t0 = 1_000_000.0;
        l.resting = Some(stale_resting_exit(5, MAX_RESTING_S + 1.0));
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        assert!(l.rotate.take().is_some(), "the slot has been held too long");
        l.resting = None;

        // Cycle 1 after the pull: the challenger is picked. In the live process
        // this is the cycle that asks for the yield and is refused.
        let e = l.target(&c, t0 + 5.0 * DEBOUNCE_S, 0).expect("a candidate");
        assert_eq!(e.market_id, "K-b", "the slot changes hands");
        // Cycle 2: NOTHING RESTED, so the claim still stands. This is the
        // assertion that fails against the old one-shot rule — it returned
        // "K-a" here and the incumbent was back.
        let e = l.target(&c, t0 + 6.0 * DEBOUNCE_S, 0).expect("a candidate");
        assert_eq!(
            e.market_id, "K-b",
            "the incumbent must not reclaim the slot on the cycle after a refusal"
        );
    }

    /// ...BUT IT IS BOUNDED, or a challenger that can never be priced locks the
    /// incumbent out for ever — a worse starvation than the one above, because
    /// an empty slot earns nothing at all while the incumbent's ask at least
    /// pays if somebody lifts it.
    #[test]
    fn a_hand_over_nobody_takes_up_lapses_after_its_budget() {
        let mut l = Live::new(true, "/dev/null".into());
        let mut other = cand(10, 2.0);
        other.market_id = "K-b".into();
        let c = [cand(10, 1.0), other];
        let t0 = 1_000_000.0;
        l.resting = Some(stale_resting_exit(5, MAX_RESTING_S + 1.0));
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        assert!(l.rotate.take().is_some());
        l.resting = None;

        for i in 0..HANDOVER_CYCLES {
            let e = l.target(&c, t0 + (5.0 + f64::from(i)) * DEBOUNCE_S, 0).expect("a candidate");
            assert_eq!(e.market_id, "K-b", "still the challenger's claim on selection {i}");
        }
        let e = l
            .target(&c, t0 + (5.0 + f64::from(HANDOVER_CYCLES)) * DEBOUNCE_S, 0)
            .expect("a candidate");
        assert_eq!(e.market_id, "K-a", "the claim lapses and the incumbent may have it back");
        assert!(l.served.is_empty(), "and the claim is cleared, not merely ignored");
    }

    /// A HAND-OVER WITH NOBODY TO HAND TO KEEPS THE INCUMBENT.
    ///
    /// An empty slot is worse than a re-bought queue position, so the skip
    /// applies only while something else can actually use it.
    #[test]
    fn a_rotated_out_market_is_chosen_again_when_it_is_the_only_one_left() {
        let mut l = Live::new(true, "/dev/null".into());
        let c = [cand(10, 1.0)];
        let t0 = 1_000_000.0;
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        l.served = vec!["K-a".into()];
        let e = l.target(&c, t0 + 4.0 * DEBOUNCE_S, 0).expect("the only candidate");
        assert_eq!(e.market_id, "K-a");
        assert!(l.served.is_empty(), "the lap completed rather than leaving the slot empty");
    }

    /// THE SLOT GOES ROUND THE WHOLE HELD SET, NOT BETWEEN THE TOP TWO.
    ///
    /// Excluding only the incumbent is not a rotation. `select`'s ordering is
    /// stable, so the runner-up wins every hand-over and the pair trade the slot
    /// for ever while everything behind them waits — live over the 27 h to
    /// 2026-08-24 20:00 that was 31 exits placed, 0 filled, three names sharing
    /// the slot and the log itself reporting "8 other held candidate(s)
    /// waiting".
    ///
    /// Three candidates, each rotated out in turn: the third must get the slot
    /// before the first sees it again, and the lap must then reset.
    #[test]
    fn the_slot_laps_the_held_set_before_repeating() {
        let mut l = Live::new(true, "/dev/null".into());
        let mut b = cand(10, 2.0);
        b.market_id = "K-b".into();
        let mut d = cand(10, 3.0);
        d.market_id = "K-c".into();
        // `K-a` sorts first, so it is the incumbent and the one that would
        // reclaim the slot on every hand-over under the old one-name rule.
        let c = [cand(10, 1.0), b, d];
        let t0 = 1_000_000.0;
        // Warm the debounce: nothing is a candidate until it has been held for
        // DEBOUNCE_SCANS scans, and `waiting` counts only admitted rows.
        let mut tick = 0.0f64;
        l.resting = Some(stale_resting_exit_on("K-a", 5, MAX_RESTING_S + 1.0));
        for _ in 0..4 {
            let _ = l.target(&c, t0 + tick * DEBOUNCE_S, 0);
            tick += 1.0;
        }
        let mut seen = Vec::new();
        for holder in ["K-a", "K-b", "K-c"] {
            // Whoever holds the slot has held it too long; rotate them out.
            l.resting = Some(stale_resting_exit_on(holder, 5, MAX_RESTING_S + 1.0));
            let _ = l.target(&c, t0 + tick * DEBOUNCE_S, 0);
            tick += 1.0;
            assert!(l.rotate.take().is_some(), "{holder} has held the slot too long");
            l.resting = None;
            let e = l.target(&c, t0 + tick * DEBOUNCE_S, 0).expect("a candidate");
            tick += 1.0;
            seen.push(e.market_id.clone());
        }
        assert_eq!(
            seen,
            vec!["K-b".to_string(), "K-c".to_string(), "K-a".to_string()],
            "every held candidate takes a turn before the first repeats"
        );
    }

    /// A HALTED MARKET DOES NOT SHADOW THE LIVE ONES BEHIND IT.
    ///
    /// `target` used to take the first admitted candidate and THEN refuse if it
    /// was parked, which spent the whole cycle on a market that could not take
    /// an order — 32 of them on 2026-08-20, all `KXFRENCHPRES-27-BRET`.
    #[test]
    fn a_parked_market_at_the_head_of_the_queue_is_skipped_not_fatal() {
        let mut l = Live::new(true, "/dev/null".into());
        let mut other = cand(10, 2.0);
        other.market_id = "K-b".into();
        let c = [cand(10, 1.0), other];
        let t0 = 1_000_000.0;
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        l.park("K-a");
        let e = l.target(&c, t0 + 4.0 * DEBOUNCE_S, 0).expect("K-b is live and admitted");
        assert_eq!(e.market_id, "K-b");

        // ...and when EVERY held candidate is halted, the refusal says so.
        l.park("K-b");
        let why = l.target(&c, t0 + 5.0 * DEBOUNCE_S, 0).expect_err("nothing can take an order");
        assert!(why.contains("halted at the venue"), "{why}");
        assert!(why.contains("K-a") && why.contains("K-b"), "it names them: {why}");
    }

    /// ...BUT NOT WHEN IT IS THE ONLY CANDIDATE.
    ///
    /// Rotating costs the ask its queue position at the venue. Paying that to
    /// re-rest the SAME exit at the SAME price is a pure loss, so age alone must
    /// not pull: the slot is only scarce when something else wants it.
    #[test]
    fn a_stale_ask_keeps_its_queue_position_when_nothing_else_wants_the_slot() {
        let mut l = Live::new(true, "/dev/null".into());
        // The only held candidate is the resting market's own.
        let c = [cand(10, 1.0)];
        let t0 = 1_000_000.0;
        l.resting = Some(stale_resting_exit(5, MAX_RESTING_S * 10.0));
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        assert!(
            l.rotate.is_none(),
            "an uncontested slot is not scarce; rotating would just buy back our own queue \
             position at full price"
        );
    }

    /// A YOUNG ASK KEEPS THE SLOT EVEN WHEN CONTESTED — the bound is an
    /// opportunity cost, not a preference for whoever asked last.
    #[test]
    fn a_young_ask_is_not_rotated_off_a_contested_slot() {
        let mut l = Live::new(true, "/dev/null".into());
        let mut other = cand(10, 2.0);
        other.market_id = "K-b".into();
        let c = [cand(10, 1.0), other];
        let t0 = 1_000_000.0;
        l.resting = Some(stale_resting_exit(5, MAX_RESTING_S - 1.0));
        for i in 0..4 {
            let _ = l.target(&c, t0 + f64::from(i) * DEBOUNCE_S, 0);
        }
        assert!(l.rotate.is_none(), "it has not had its hour yet");
    }

    // ---- the self-heal ----------------------------------------------------

    /// The sink a `rest-kalshi` heal never reaches: that shape's open leg is the
    /// PM-US one, so the Kalshi sink is present only to satisfy the signature.
    /// A `FakeVenue` records every call, so if `heal` ever DID touch it the
    /// tests asserting `calls()` would say so.
    fn kalshi_stub() -> std::sync::Arc<dyn crate::sink::OrderSink> {
        FakeVenue::with_fills(&[0])
    }

    fn pending_for(qty: i64, attempts: u32) -> PendingClose {
        PendingClose {
            order: resting_exit(qty).order,
            filled: qty,
            since: Instant::now(),
            attempts,
        }
    }

    /// **THE RETRY IS SIZED AT THE VENUE, NEVER AT `filled`.**
    ///
    /// This is the arithmetic the whole self-heal rests on, and getting it wrong
    /// is worse than not healing at all. Two of the five ways a close fails
    /// leave us unsure whether it traded — the place task failing, and an
    /// accepted IOC whose fill could not be read. Re-sending `filled` on either
    /// buys the leg TWICE and turns a naked short into a naked long.
    #[test]
    fn a_retry_is_sized_to_the_venue_shortfall_and_never_to_the_fill() {
        let recs = vec![open_basket(1.0, 5, "0.22", "0.19")];
        let p = pending_for(5, 1);
        // Ledger claims 5 NO; venue confirms 5 short. Nothing was bought back,
        // so the whole 5 is still owed.
        assert_eq!(close_shortfall(&recs, &p, -5.0), 5, "nothing closed yet");
        // The IOC we could not read actually filled 3: venue is short 2.
        assert_eq!(close_shortfall(&recs, &p, -2.0), 2, "only the remainder is owed");
        // ...and if it filled the lot, NOTHING is owed. Re-sending `filled`
        // here is the naked-long bug this exists to prevent.
        assert_eq!(close_shortfall(&recs, &p, 0.0), 0, "the close completed after all");
        // A position that has gone LONG is not ours to buy more of either.
        assert_eq!(close_shortfall(&recs, &p, 3.0), 0, "never buy into a long");
    }

    /// A SHORTFALL OF ZERO IS A SUCCESS, AND IT IS BOOKED.
    ///
    /// The unreadable-IOC case: the close DID complete, we just could not see
    /// it. The position is right and only the ledger is wrong, so the heal is a
    /// pure bookkeeping act — and it must still write the `unwound` record, or
    /// the exposure fold keeps counting a basket that is closed.
    #[tokio::test]
    async fn a_heal_that_finds_nothing_owed_books_the_unwind_and_clears_the_latch() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::holding(&pmus, "p-a", 0.0); // flat: the close landed
        let pm: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("heal-done", 5);
        let mut live = Live::new(false, path.clone());
        live.pending = Some(pending_for(5, 0));
        let healed_before = healed();
        let out = heal(&mut live, &view("0.20"), &kalshi_stub(), &pm).await.join("\n");

        assert!(live.pending.is_none(), "the latch is cleared: {out}");
        assert_eq!(healed(), healed_before + 1);
        assert!(out.contains("completed after all"), "{out}");
        // No order is sent to fix a position that is already right.
        assert!(!pmus.calls().iter().any(|c| c.starts_with("place")), "{:?}", pmus.calls());
        let recs = crate::ledger::read(&path).expect("clean ledger");
        assert_eq!(recs.len(), 2, "{recs:?}");
        assert_eq!(recs[1]["status"], "unwound");
        assert_eq!(recs[1]["qty"], 5);
        let _ = std::fs::remove_file(&path);
    }

    /// AN UNREADABLE POSITIONS READ HOLDS THE LEG. It does not guess a size.
    ///
    /// The same rule as `resolve_vanished`: no evidence, no action. Sending an
    /// IOC sized on a guess is how the naked short becomes a naked long.
    #[tokio::test]
    async fn a_heal_that_cannot_read_positions_sends_nothing_and_stays_latched() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let pmus = FakeVenue::with_fills(&[0]); // `net` unset -> refuses
        let pm: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let mut live = Live::new(false, ledger_scratch("heal-blind"));
        live.pending = Some(pending_for(5, 0));
        let out = heal(&mut live, &view("0.20"), &kalshi_stub(), &pm).await.join("\n");

        assert!(live.pending.is_some(), "still latched: {out}");
        assert!(out.contains("no honest size"), "{out}");
        assert!(!pmus.calls().iter().any(|c| c.starts_with("place")), "{:?}", pmus.calls());
    }

    /// INSIDE THE CHEAP WINDOW A CLOSE THAT DOES NOT PAY IS LEFT ALONE...
    #[tokio::test]
    async fn a_heal_inside_the_profitable_window_waits_rather_than_crossing() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::holding(&pmus, "p-a", -5.0); // all 5 still owed
        let pm: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("heal-wait", 5);
        let mut live = Live::new(false, path.clone());
        live.pending = Some(pending_for(5, 0));
        // A PM-US ask of 0.60 puts the close far above what the basis allows.
        let out = heal(&mut live, &view("0.60"), &kalshi_stub(), &pm).await.join("\n");

        assert!(live.pending.is_some(), "still owed, still latched: {out}");
        assert!(out.contains("does not pay yet"), "{out}");
        assert!(!pmus.calls().iter().any(|c| c.starts_with("place")), "{:?}", pmus.calls());
        let _ = std::fs::remove_file(&path);
    }

    /// ...AND PAST IT, IT CROSSES OUT AND TAKES THE LOSS.
    ///
    /// The whole point of the change. Profitable-only is what
    /// `--positions-recon-act` already is, and `positions_recon_acted` is 0
    /// across thousands of refusals over the life of the deployment — a leg that
    /// is underwater fails a profit floor EVERY time. A self-heal that stopped
    /// at the cheap window would inherit that 0% rate in exactly the case that
    /// needs it, and the leg would go on waiting for a human.
    #[tokio::test]
    async fn a_heal_past_the_profitable_window_crosses_out_and_says_it_is_taking_a_loss() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let pmus = FakeVenue::with_fills(&[5]);
        FakeVenue::holding(&pmus, "p-a", -5.0);
        let pm: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("heal-cross", 5);
        let mut live = Live::new(false, path.clone());
        live.pending = Some(pending_for(5, HEAL_PROFITABLE_CYCLES));
        let out = heal(&mut live, &view("0.60"), &kalshi_stub(), &pm).await.join("\n");

        assert!(out.contains("CROSSING OUT"), "{out}");
        assert!(out.contains("realises a loss"), "and it says so plainly: {out}");
        // It takes the book one tick THROUGH the ask, so the IOC actually clears.
        let ps = pmus.calls();
        assert!(ps.iter().any(|c| c.starts_with("place p-a 5x @0.61")), "{ps:?}");
        assert!(live.pending.is_none(), "flat, so the latch clears: {out}");
        let recs = crate::ledger::read(&path).expect("clean ledger");
        assert_eq!(recs[1]["status"], "unwound");
        let _ = std::fs::remove_file(&path);
    }

    /// THE LATCH CLEARS ON EVIDENCE, NOT ON ATTEMPTS. A retry that fills only
    /// part of what is owed leaves the leg parked, and the next cycle
    /// re-measures against the venue rather than trusting this one's arithmetic.
    #[tokio::test]
    async fn a_partial_heal_stays_latched_and_re_measures_next_cycle() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let pmus = FakeVenue::with_fills(&[2]); // asked for 5, got 2
        FakeVenue::holding(&pmus, "p-a", -5.0);
        let pm: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("heal-partial", 5);
        let mut live = Live::new(false, path.clone());
        live.pending = Some(pending_for(5, HEAL_PROFITABLE_CYCLES));
        let out = heal(&mut live, &view("0.60"), &kalshi_stub(), &pm).await.join("\n");

        assert!(out.contains("filled 2 of 5"), "{out}");
        assert!(live.pending.is_some(), "not flat, so not cleared: {out}");
        assert_eq!(crate::ledger::read(&path).expect("ledger").len(), 1, "nothing booked yet");
        let _ = std::fs::remove_file(&path);
    }

    /// THE RATCHET IS NOT DECREMENTED, AND THE LATCH IS THE PAIR.
    ///
    /// `maker_exit_unresolved` answers "how many times have we been one-legged
    /// after an exit" and only ever climbs; `outstanding()` answers "are we naked
    /// right now" and is what gates new exits. Collapsing the two into one
    /// falling gauge would lose the first question — which is the one that says
    /// whether the close path is unreliable rather than unlucky.
    ///
    /// (It would NOT break the page. That claim was in an earlier draft and is
    /// false: `gauge_deltas.py`'s RISE rule re-clamps its baseline with
    /// `base = min(base, cur)`, and driven through 0 -> 1 -> 0 -> 1 it pages
    /// both times. The reason here is legibility, not alerting.)
    #[tokio::test]
    async fn healing_leaves_the_incident_count_up_and_clears_only_the_latch() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let pmus = FakeVenue::with_fills(&[0]);
        FakeVenue::holding(&pmus, "p-a", 0.0);
        let pm: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_with_open("heal-ratchet", 5);
        let mut live = Live::new(false, path.clone());
        live.pending = Some(pending_for(5, 0));
        let before = unresolved();
        let out_before = outstanding();
        heal(&mut live, &view("0.20"), &kalshi_stub(), &pm).await;

        assert_eq!(unresolved(), before, "the incident count is a RATCHET and does not fall");
        assert_eq!(outstanding(), out_before.saturating_sub(1), "but the LATCH clears");
        let _ = std::fs::remove_file(&path);
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

    /// THE MIRROR OF THE TEST ABOVE, AND IT CAUGHT A REAL BUG. Every venue
    /// call a `rest-pmus` exit makes about its RESTING order must go to PM-US,
    /// because PM-US is the only venue that has ever heard of that order id.
    ///
    /// The post-cancel re-read was hardcoded to the Kalshi sink — correct while
    /// Kalshi was the only venue an exit could rest on, and silently wrong the
    /// moment it was not. It does not fail loudly: `filled_qty` on a foreign id
    /// errors, the error falls through to "use the pre-cancel lower bound", and
    /// a fill that raced the cancel is closed short. That is a naked leg
    /// arrived at through the code path whose entire purpose is preventing one,
    /// and no type could catch it because both sinks are `dyn OrderSink`.
    ///
    /// Asserting on which FakeVenue was called is the only thing that can.
    #[tokio::test]
    async fn a_rest_pmus_partial_fill_re_reads_pmus_and_never_touches_kalshi() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        // The PM-US bid is the RESTING leg: 2 filled on the first read, 3 by the
        // time the cancel lands. Kalshi is the CLOSE leg and takes the IOC.
        let pmus = FakeVenue::with_fills(&[2, 3]);
        let kalshi = FakeVenue::with_fills(&[3]);
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let p: std::sync::Arc<dyn crate::sink::OrderSink> = pmus.clone();

        let path = ledger_scratch("partial-pmus");
        let mut live = Live::new(false, path.clone());
        let mut r = resting_exit(5);
        r.order.shape = Shape::RestPmUs;
        // A PM-US bid at 0.25, closing by selling Kalshi into its bid.
        r.order.limit = "0.2500".into();
        live.resting = Some(r);
        let out = manage(&mut live, &wide_pm_view("0.30", "0.24", "0.60"), &k, &p).await.join("\n");

        // THE RESTING LEG: read, cancel, read again — all three on PM-US.
        let ps = pmus.calls();
        let cancel_at =
            ps.iter().position(|c| c == "cancel").unwrap_or_else(|| panic!("no cancel: {ps:?}"));
        assert_eq!(cancel_at, 1, "the cancel follows the FIRST read: {ps:?}");
        assert_eq!(ps.len(), 3, "and a SECOND read follows the cancel: {ps:?}");
        assert_eq!(ps[2], "filled_qty -> 3", "the re-read went to PM-US: {ps:?}");

        // THE CLOSE LEG: Kalshi sees the IOC and NOTHING before it. A
        // `filled_qty` here would be this bug: the id is not Kalshi's.
        let ks = kalshi.calls();
        assert!(
            ks.first().is_some_and(|c| c.starts_with("place K-a 3x")),
            "Kalshi's first call must be the close IOC for the RE-READ count, not a read of a \
             PM-US order id: {ks:?}"
        );
        assert!(
            !ks.iter().any(|c| c.starts_with("filled_qty")
                && ks.iter().position(|x| x == c) < ks.iter().position(|x| x.starts_with("place"))),
            "nothing may be read on Kalshi before the close: {ks:?}"
        );

        assert!(live.resting.is_none(), "a spent exit must not be polled next cycle");
        assert!(out.contains("cancelling the remainder"), "{out}");

        let recs = crate::ledger::read(&path).expect("clean ledger");
        assert_eq!(recs.len(), 1, "{recs:?}");
        assert_eq!(recs[0]["status"], "unwound");
        assert_eq!(recs[0]["qty"], 3, "booked the re-read count, not the pre-cancel one");
        assert_eq!(recs[0]["maker_exit_shape"], "rest-pmus");
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

    // ---- the stand-off ----------------------------------------------------

    /// OUR ASK ON A MARKET THE ENGINE OWES A HEDGE ON MAKES THE HEDGE THE TAKER,
    /// and `taker_at_cross` cancels the taker — so the hedge quietly does not
    /// happen and the leg it was covering stays naked. `naked_act` asks this
    /// engine the same question before it acts; this is that question asked from
    /// the other side.
    ///
    /// It is an ordinary refusal on the ordinary gauge, unlike the unresolved
    /// latch: the obligation is discharged in seconds and the exit may then rest.
    #[tokio::test]
    async fn an_exit_is_not_rested_where_the_engine_already_owes_a_hedge() {
        let _g = allow_all().await;
        crate::naked_act::publish_inflight(BTreeSet::from(["K-a".to_string()]));
        let (mut cx, fees) = ready();
        let recs = vec![open_basket(1.0, 34, "0.22", "0.19")];
        let before = refused();
        let e = decide(
            &mut cx, &fees, &recs, &cand(34, 1.0), "p-a",
            &quote(Some("0.10"), Some("0.30")), &view("0.20"), Instant::now(),
            false,
        )
        .expect_err("the engine owes a hedge on this market");
        assert!(e.contains("double hedge"), "the registry's own words: {e}");
        assert!(e.contains("taker_at_cross"), "and why it matters here: {e}");
        assert_eq!(refused(), before + 1, "'not now' is a refusal, not a halt");
        crate::naked_act::publish_inflight(BTreeSet::new());
    }

    /// THE CRUX: THE HALT MUST NOT ALSO STRAND THE LEG IT CREATED.
    ///
    /// While an exit is working a market the backstop is stood off it, because a
    /// recon IOC that crosses our own post-only ask is CANCELLED by
    /// `taker_at_cross` and reported as the book having moved. The instant that
    /// exit fails its close the opposite is true: the leg it just left naked is
    /// exactly what the backstop is for, and nothing may be standing in front of
    /// it.
    ///
    /// NOTHING HERE IMPLEMENTS THAT HAND-OVER. It falls out of two things that
    /// are already true — `close_leg` clears `resting` before anything can
    /// fail, and the latch stops `target` putting the market back — plus the
    /// working set being rebuilt from [`Live`] on every cycle, which makes the
    /// release at most one cycle wide. This test is what keeps the derivation
    /// honest when any of the three moves.
    #[tokio::test]
    async fn an_exit_that_halted_releases_the_stand_off_so_the_backstop_can_run() {
        let _g = allow_all().await;
        arm_standoff();
        let mut l = Live::new(false, "/dev/null".into());
        l.resting = Some(resting_exit(5));
        publish_working(l.working_set(None));
        let why = working_check("K-a").expect_err("an ask of ours is resting there");
        assert!(why.contains("taker_at_cross"), "it says what would happen: {why}");

        // The close failed: the ask is spent and forgotten, and the module is
        // latched off for good.
        l.resting = None;
        let c = [cand(10, 1.0)];
        assert!(l.target(&c, 1_000_000.0, 1).is_err(), "the latch holds this module off");
        publish_working(l.working_set(None));
        assert!(
            working_check("K-a").is_ok(),
            "the backstop must be free to complete the leg this module just left naked"
        );
        reset_standoff();
    }

    /// AN ORDER WE CANNOT ADDRESS KEEPS THE STAND-OFF ON, AND NOTHING TAKES IT
    /// OFF AGAIN.
    ///
    /// A place whose answer never came may be an ask resting under an id this
    /// process never learned. It cannot be cancelled and it cannot be polled, so
    /// the only thing left to do about it is refuse to send anything of ours
    /// across it — for the rest of the process's life, because nothing here can
    /// ever learn that it is gone.
    #[tokio::test]
    async fn an_unaddressable_order_keeps_the_stand_off_on() {
        let _g = allow_all().await;
        arm_standoff();
        let kalshi = FakeVenue::with_fills(&[0]);
        *kalshi.place_err.lock().unwrap() =
            Some(arb_venue::VenueError::Transport("timed out".into()));
        let k: std::sync::Arc<dyn crate::sink::OrderSink> = kalshi.clone();
        let mut live = Live::new(false, "/dev/null".into());
        let out =
            place(&mut live, resting_exit(5).order, &view("0.20"), &k, &kalshi_stub())
                .await
                .join("\n");
        assert!(out.contains("PLACE DID NOT COMPLETE"), "{out}");
        assert!(live.resting.is_none(), "nothing addressable means nothing remembered");
        publish_working(live.working_set(None));
        let why = working_check("K-a").expect_err("an ask of ours may be resting there");
        assert!(why.contains("K-a"), "{why}");
        reset_standoff();
    }

    /// A SILENT LOOP IS NOT AN EMPTY ONE — AND AN UNARMED ONE IS.
    ///
    /// Fail-closed once something is armed, fail-open before that, which is not
    /// `naked_act::inflight_check`'s rule and is deliberate: with no armed exit
    /// there is no ask of ours to cross, so refusing would cost a real naked leg
    /// its completion for a collision that cannot happen.
    #[tokio::test]
    async fn a_silent_maker_exit_loop_refuses_rather_than_assuming_the_ask_is_gone() {
        let _g = allow_all().await;
        assert!(working_check("K-a").is_ok(), "nothing armed, so nothing to collide with");
        arm_standoff();
        let why = working_check("K-a").expect_err("armed, and it has never said what it holds");
        assert!(why.contains("has not yet published"), "{why}");
        reset_standoff();
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
