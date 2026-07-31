//! The hedge: an obligation's life from the maker fill that mints it to the
//! attempt that discharges it.
//!
//! `PendingHedge` carries the accounting contract (I1-I5) this whole module
//! exists to maintain, with the live defect each invariant was written to kill
//! named at its own site. The policy — may this price be taken, is this retry
//! due, is this leg naked — is pure functions, because it was reachable only
//! through a `tokio::select!` arm over live channels when all four of those
//! defects were written.

use super::fill::{UnclaimedFill, FILL_ACK_GRACE};
use super::{Engine, HedgeRetry};
use arb_core::book::BookBuilder;
use arb_core::fill::HedgeAnchor;
use arb_core::intent::{self, Intent, Tag};
use arb_core::model::{BookSide, Venue};
use arb_core::scan::{Cx, Rel};
use std::collections::HashMap;

/// THE HEDGE ACCOUNTING INVARIANT — what must hold between maker fills, hedge
/// obligations, hedge fills and booked baskets. Everything below enforces it,
/// and the three defects it was written to kill are named at each site.
///
/// For one maker order `M`, each INCREASE in `M`'s cumulative filled count is
/// one OBLIGATION `o` (`arb_core::fill` mints exactly one per increase):
///
/// ```text
///   o.owed     contracts that increase filled — set ONCE, never recomputed
///   o.filled   contracts hedged for o, summed over EVERY attempt made for it
///   o.anchor   where the hedge leg stood when the basket was proven profitable
///   booked(o)  contracts written to the trade ledger against o
///
///   I1  0 <= o.filled <= o.owed                       never over-hedge
///   I2  booked(o) == min(o.filled, o.owed)            the ledger IS the hedge,
///                                                     up to what was owed
///   I3  o.filled < o.owed  =>  o is live in `pending_hedges`, its retry asks
///                              for exactly o.owed - o.filled, and its naked
///                              alarm is still armed
///   I4  sum of o.owed over M's obligations == M's cumulative filled count
///   I5  every attempt's slip is measured against o.anchor — never against the
///       previous attempt's price
/// ```
///
/// Read I1 and I2 exactly as written, because both have deliberate edges:
///   * I1 is violated TRANSIENTLY. `o.filled` is credited the full frame delta
///     before the obligation is retired, so it can exceed `o.owed` for the rest
///     of that frame's handling. Harmless only because `over > 0` implies
///     `done`, i.e. an obligation that over-fills is retired in the same breath
///     — `hedge_credit` pins that, and a change there could make I1 durable.
///   * I2 is NOT `booked(o) == o.filled`. The `over` contracts are filled and
///     deliberately NOT booked: there is no maker fill to pair them with, so a
///     basket record would invent one. They are alarmed for a human instead
///     (`hedges_overfilled`), and they also never reach `RiskView::record_open`,
///     so the concentration caps under-count them until that reconciliation.
///   * I3 says nothing about obligations with NO anchor. Those never enter
///     `pending_hedges` at all: the obligation is left unconsumed on purpose so
///     `dropped_unconsumed()` surfaces it. That is the one exposure this
///     structure does not track, and it is tracked by arb-core instead.
///
/// Each violation was a live bug (audit 2026-07-28):
///   * I1/I2 — a hedge was retired from `hedge_orders` on its FIRST fill frame,
///     so a 10-lot filling 4-then-6 booked 4 and understated exposure forever.
///     Both venues report multi-frame: Kalshi sends one frame per trade (its own
///     feed test walks cum 2 then cum 5), PM-US sends PARTIAL_FILL then FILL.
///   * I2/I3 — the credit was keyed by the MAKER order but compared against ONE
///     obligation's qty, so two partial fills on one maker order shared a credit
///     and the second obligation's hedge was retired as "already hedged".
///   * I3 — the remainder after a partial fill lost BOTH its retry and its naked
///     alarm, and `missing = qty - done` subtracted a cumulative credit from an
///     already-net remainder, so the third retry under-asked.
///   * I3 — and both halves of it froze when the market feed died, because the
///     retry interval and the alarm horizon were measured in TAPE time. See
///     `first_at`/`last_try_at`.
///   * I5 — the anchor was overwritten with each attempt's price, so `max_slip`
///     applied per attempt: ~12c given up before the 60s alarm on a 1c policy
///     (12 retries at 5s inside a 60s alarm, each free to give up the full 1c).
///
/// The obligation is named by the id of its FIRST attempt (its "chain id"),
/// which is the only name that stays stable across retries.
pub(super) struct PendingHedge {
    /// The maker order whose fill created this obligation (ledger attribution).
    pub(super) maker_order_id: String,
    /// Contracts owed. Set once from the maker fill delta; NEVER recomputed
    /// from an attempt's size, which is where the double-subtraction came from.
    pub(super) owed: i64,
    /// Contracts hedged so far, over every attempt in the chain.
    pub(super) filled: i64,
    /// Where the hedge leg stood when the basket was proven profitable. Held
    /// for the LIFE of the obligation: it is the only price `max_slip` may be
    /// measured against, and it also carries the hedge leg's venue/market/side.
    pub(super) anchor: HedgeAnchor,
    /// MONOTONIC time the obligation was created — how long we have been naked.
    ///
    /// NOT tape time, for exactly the reason `ParkedCancel::since` is not: tape
    /// time stops advancing the moment the market feed dies, and the fill feed is
    /// a SEPARATE socket that keeps delivering. A maker fill sets the obligation's
    /// clocks from its own event time, so with a dead market feed
    /// `now - first_ts` and `now - last_try_ts` both stayed pinned at 0 — no
    /// retry ever became due and the naked alarm could never fire, for as long as
    /// the feed stayed down. The armed process was dropped by the feed three times
    /// on 2026-07-28 (13:20:10, 13:28:41, 13:56:15), and the first-attempt slip
    /// gate makes it worse than it was: attempt 1 can now legitimately be
    /// refused, so a frozen clock means never retried AND never alarmed.
    pub(super) first_at: std::time::Instant,
    /// MONOTONIC time the last attempt was PLACED — same reasoning. `interval_s`
    /// gates placements, so this only moves on a real place.
    pub(super) last_try_at: std::time::Instant,
    /// Our id for the most recent attempt, or `None` if none was ever placed
    /// (the first attempt can be refused by the slip gate). Read to decide
    /// whether this obligation has anything at the venue whose `order_ack` could
    /// still be in flight.
    pub(super) latest_attempt: Option<String>,
    pub(super) tries: u32,
    pub(super) alarmed: bool,
    /// Whether the ack-hold has already been logged for this obligation. One
    /// line per obligation: the decision is re-taken every second.
    pub(super) hold_logged: bool,
    /// MONOTONIC time before which no further attempt will be PLACED, because
    /// the venue said the market is halted (`Retry::MarketHalted`). `None` until
    /// a place is refused that way.
    ///
    /// A HALT IS NOT A PRICE PROBLEM, and `interval_s` is sized for one: it is
    /// "comfortably longer than a fill report takes", i.e. tuned against a book
    /// that is trading. Against a venue that has stopped trading the market
    /// entirely, the same interval is a request every 5s that cannot succeed —
    /// 335 of them over 31 minutes on 2026-07-30, all identical, all spending
    /// the SHARED `Priority::Critical` budget that the next real hedge and the
    /// halt sweep both draw on.
    ///
    /// Parking is deliberately NOT going quiet. The obligation stays in
    /// `pending_hedges`, stays owed, and keeps its naked alarm — `hedge_tick_plans`
    /// computes the alarm independently of the plan, and `HedgePlan::Parked` is
    /// not `Retire`. A parked leg is exactly as naked as an unparked one, and
    /// silence about it would be strictly worse than the storm.
    pub(super) parked_until: Option<std::time::Instant>,
    /// Consecutive places on this obligation the venue refused as halted. Drives
    /// the backoff step (see `venue_reopen_park`), and is what makes this a
    /// BACKOFF rather than a second fixed interval.
    pub(super) paused_strikes: u32,
}

/// The first park after a halted refusal, and the ceiling the doubling stops at.
///
/// SEPARATE CONSTANTS, not multiples of `interval_s`, for the reason
/// `HOLD_FOR_ACK` is separate from `--hedge-alarm-s`: an operator tuning the
/// retry interval against book latency is not thereby making a decision about
/// how long to wait out a venue halt, and tying the two means they cannot say
/// one without saying the other.
///
/// The ceiling is what bounds the cost of being wrong. A halt ends at a moment
/// nothing tells us about, so the park is also the WORST-CASE delay between the
/// market reopening and our first attempt on it — paid in naked time. 60s is
/// chosen against the only other thing on this box that closes such a leg, the
/// Python hedger's 5-minute timer (`arbbot-hedge.timer`): a ceiling under that
/// keeps this engine the likelier of the two to hedge its own obligation.
///
/// Against the observed 31-minute halt this is 15/30/60/60/... — 33 places where
/// there were 335, with at most 60s of added naked time at the end.
const VENUE_REOPEN_PARK_FIRST: std::time::Duration = std::time::Duration::from_secs(15);
const VENUE_REOPEN_PARK_MAX: std::time::Duration = std::time::Duration::from_secs(60);

/// How long to park after the `strikes`-th consecutive halted refusal.
///
/// Doubling, capped. `strikes` is 1-based (the first refusal is strike 1); 0
/// is not reachable from the caller and answers with the first step rather
/// than with 0, because a park of 0 is the storm.
pub(crate) fn venue_reopen_park(strikes: u32) -> std::time::Duration {
    let steps = strikes.saturating_sub(1).min(16); // 2^16 * 15s already > the cap
    (VENUE_REOPEN_PARK_FIRST * 2u32.saturating_pow(steps)).min(VENUE_REOPEN_PARK_MAX)
}

/// May a retry take `touch`, given the anchor the basket was priced against?
///
/// The anchor is the price at which the basket was known profitable, so
/// `max_slip` is exactly how much of that edge we will surrender to stop being
/// naked. Beyond it the answer is WAIT, never chase — Geoff 2026-07-22, "hedge
/// only if profitable; otherwise find a profitable hedge in the future". The
/// naked alarm is what keeps waiting from being silent.
///
/// `book_side` is the side of the hedge leg's book we take: `Bid` means we are
/// SELLING into it (worse = lower), `Ask` means BUYING from it (worse = higher).
/// It was a `&str`, and the arms below were an `if == "bid"` with the buy case
/// as the else — so a side that was neither made the budget run the wrong way.
fn hedge_price_acceptable(
    cx: &mut Cx,
    book_side: BookSide,
    touch: &str,
    anchor: &str,
    max_slip: &str,
) -> bool {
    let a = cx.parse_exact(anchor);
    let slip = cx.parse_exact(max_slip);
    let t = cx.parse_exact(touch);
    match book_side {
        BookSide::Bid => {
            let floor = cx.sub(a, slip);
            cx.cmp(t, floor) != std::cmp::Ordering::Less
        }
        BookSide::Ask => {
            let ceil = cx.add(a, slip);
            cx.cmp(t, ceil) != std::cmp::Ordering::Greater
        }
    }
}

/// One ATTEMPT at a hedge, kept so its fill can be recognised, credited to the
/// obligation it was placed for, and booked. Every attempt in a chain has its
/// own entry, and entries are never removed: a late frame on a superseded
/// attempt must stay recognisable, or it reads as unexplained money.
#[derive(Clone)]
pub(super) struct HedgeOrder {
    /// The maker order whose fill created this hedge.
    pub(super) maker_order_id: String,
    /// The OBLIGATION this attempt covers (`pending_hedges`' key) — stable
    /// across retries, so a fill on attempt 3 credits what attempt 1 owed.
    pub(super) chain_id: String,
    pub(super) market_id: String,
    pub(super) venue: Venue,
    /// The ORDER side — what we send — not the book side we take. `taking_side`
    /// is the only thing that produces one from the other.
    pub(super) side: BookSide,
    pub(super) price: String,
    pub(super) qty: i64,
    /// Cumulative contracts THIS attempt has been reported filled for. Venue
    /// reports are cumulative and arrive over several frames, so only the
    /// increase is credited — the same rule `arb_core::fill` applies to makers.
    pub(super) cum_filled: i64,
    /// OUR id for the attempt this one supersedes — `None` for the first in a
    /// chain, which supersedes nothing.
    ///
    /// It exists because the retry cannot be trusted to know that attempt is
    /// dead. A hedge is never in the `FillLedger` (registering one would let
    /// its own fill mint another hedge), so the fill FEED is the only thing
    /// that can say an attempt traded — and a Kalshi frame lost while the
    /// socket was dark is exactly what `kalshi_fill_gaps` counts. With the
    /// frame gone `filled` stays short, this retry re-places, and BOTH orders
    /// fill: long against our own short. `hedges_overfilled` does not catch it,
    /// because the second fill is credited to an obligation the first already
    /// discharged.
    ///
    /// So the executor asks the venue about this id before sending the retry —
    /// see `sink::prior_attempt`. Kept here rather than on `PendingHedge`
    /// because the obligation outlives its attempts and this is a fact about
    /// ONE attempt; `drain_intents` reads it back out by the place's order id.
    pub(super) supersedes: Option<String>,
}

/// The attempt a hedge place must reconcile against venue truth before it is
/// sent: the most recent one in its chain THAT REACHED THE VENUE, with what
/// this process has already booked against it.
///
/// WALKING BACK IS THE WHOLE POINT, and it is not defensive coding. `supersedes`
/// names the immediately preceding attempt, but that attempt may never have
/// existed at the venue — the executor withholds a retry whose verification came
/// back unreadable, `drain_intents` drops a command when the executor channel is
/// full, and an ack can still be in flight. An attempt with no venue id is
/// therefore not a hazard (nothing of it is at the venue to have filled unseen)
/// and not an answer either (the venue cannot be asked about an id it never
/// issued). The hazard is the one BEFORE it, which is still outstanding.
///
/// Stopping at the first link instead is how this fix would reintroduce the
/// defect one cycle later: h1 fills unseen, h2 is withheld and so never acked,
/// h3 names h2, h2 resolves to nothing, and h3 goes out unverified on top of h1.
/// One retry interval bought, then the same double hedge — with
/// `hedges_overfilled` still reading 0.
///
/// It terminates: `supersedes` is set once at mint from the attempt before it,
/// so the chain is strictly descending and finite. `None` means no attempt in
/// this chain has a venue id — the lost-ACK case, which is `recover_place`'s to
/// repair and not this path's to refuse.
pub(super) fn superseded(
    hedge_orders: &HashMap<String, HedgeOrder>,
    oid_venue: &HashMap<String, String>,
    order_id: &str,
) -> Option<crate::exec::Superseded> {
    let mut at = hedge_orders.get(order_id)?.supersedes.clone();
    while let Some(prior) = at {
        if let Some(vid) = oid_venue.get(&prior) {
            return Some(crate::exec::Superseded {
                venue_order_id: vid.clone(),
                // What we have ALREADY booked for that attempt. The executor
                // compares the venue's total against this, never against zero:
                // an attempt that partially filled and was credited is fully
                // accounted for, and refusing its remainder's retry would leave
                // the leg naked for ever.
                //
                // 0 if the attempt is somehow not in `hedge_orders`, which is
                // the fail-CLOSED reading — anything the venue reports then
                // exceeds it and the retry is withheld. Entries are never
                // removed from that map, so this should be unreachable.
                credited: hedge_orders.get(&prior).map_or(0, |h| h.cum_filled),
            });
        }
        at = hedge_orders.get(&prior).and_then(|h| h.supersedes.clone());
    }
    None
}

/// The order side that TAKES `book_side` of the hedge leg's book: taking a bid
/// means SELLING (an ask-side order), taking an ask means BUYING. Written once
/// because the mint path and the retry path must never disagree about it.
pub(super) fn taking_side(book_side: BookSide) -> BookSide {
    match book_side {
        BookSide::Bid => BookSide::Ask,
        BookSide::Ask => BookSide::Bid,
    }
}

/// What one hedge fill frame means. Pure, so the arithmetic that got I1/I2
/// wrong can be tested exhaustively without a runtime or a venue.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct HedgeCredit {
    /// Contracts newly filled by this frame. 0 = duplicate/stale/replayed.
    pub(super) delta: i64,
    /// Of `delta`, the part the obligation still owed — what gets BOOKED.
    pub(super) book: i64,
    /// Of `delta`, the part beyond the obligation. Contracts we hold with no
    /// maker leg to pair them with, so they are alarmed for a human rather
    /// than invented into a basket that never existed.
    pub(super) over: i64,
    /// The obligation is covered — retire it.
    pub(super) done: bool,
}

/// Credit one frame. `order_cum`/`order_qty` are the ATTEMPT's; `obligation` is
/// `(owed, filled)` for the chain, or `None` when the obligation has already
/// been retired — in which case anything new is an over-fill by definition.
pub(super) fn hedge_credit(
    cum: i64,
    order_qty: i64,
    order_cum: i64,
    obligation: Option<(i64, i64)>,
) -> HedgeCredit {
    let cum = cum.min(order_qty); // venue over-report clamps, as for makers
    let delta = (cum - order_cum).max(0);
    let none = HedgeCredit { delta: 0, book: 0, over: 0, done: false };
    if delta == 0 {
        return none; // idempotent: the frame told us nothing new
    }
    match obligation {
        Some((owed, filled)) => {
            let room = (owed - filled).max(0);
            let book = delta.min(room);
            HedgeCredit { delta, book, over: delta - book, done: filled + delta >= owed }
        }
        None => HedgeCredit { delta, book: 0, over: delta, done: false },
    }
}

/// What the hedge deadline should do about ONE outstanding obligation.
#[derive(Debug, PartialEq, Eq)]
enum HedgePlan {
    /// Not due yet. `interval_s` gates PLACEMENTS, so this is the ordinary
    /// between-attempts state.
    Hold,
    /// Deferring to an unattributed fill that might be this obligation's own —
    /// a distinct answer from `Hold` so the reason can be logged and tested.
    HoldForAck,
    /// The VENUE said this market is halted, and the park it bought has not
    /// expired. A distinct answer from `Hold` for the same reason `HoldForAck`
    /// is: the cause is different, and "not due yet" cannot be told from "the
    /// venue is refusing everything" in a log that spells them the same.
    Parked,
    /// Covered — retire the obligation.
    Retire,
    /// The book offers no price inside the slip budget (or no price at all).
    /// The obligation stays live and keeps its alarm: an unprofitable book is a
    /// WAIT, never a chase (Geoff 2026-07-22).
    Wait,
    /// Re-place exactly what is still missing, at `price`.
    Retry { qty: i64, price: String },
}

/// How long an obligation defers to an unattributed fill on its market before
/// hedging anyway.
///
/// Measured from the PLACEMENT of the attempt whose ack is missing, which is the
/// only clock that answers the actual question ("could that ack still be
/// coming?"). Same bound as `FILL_ACK_GRACE` and for the same reason — the
/// place's HTTP timeout is 15s — but a SEPARATE constant on purpose: tying it to
/// `--hedge-alarm-s` meant an operator raising that knob to quiet the logs also
/// bought a proportionally longer hold on every naked leg, which is not a
/// trade-off anyone would make deliberately.
const HOLD_FOR_ACK: std::time::Duration = FILL_ACK_GRACE;

/// The hedge-retry policy, as a pure function of the obligation's state.
///
/// Extracted because the arm it came from could not be tested at all — it lived
/// inside a `tokio::select!` over live channels, which is why all three of the
/// C3 arithmetic defects and the C8 ratchet survived in it.
///
/// `touch` is the current top of the hedge leg's book on `book_side` (the side
/// we TAKE). `anchor_price` is the obligation's, never an attempt's. The clocks
/// are MONOTONIC, never tape time — see `PendingHedge::first_at`.
#[allow(clippy::too_many_arguments)]
fn hedge_plan(
    cx: &mut Cx,
    pol: &HedgeRetry,
    owed: i64,
    filled: i64,
    anchor_price: &str,
    book_side: BookSide,
    touch: Option<&str>,
    last_try_at: std::time::Instant,
    now: std::time::Instant,
    ack_outstanding: bool,
    parked_until: Option<std::time::Instant>,
) -> HedgePlan {
    if filled >= owed {
        return HedgePlan::Retire;
    }
    let since_try = now.saturating_duration_since(last_try_at);
    if since_try.as_secs_f64() < pol.interval_s {
        return HedgePlan::Hold;
    }
    // BELOW `Retire`, ABOVE everything else. A halted venue cannot fill an
    // order at any price, so reading the book and judging the touch would only
    // decide which price not to send. Above `HoldForAck` too: that hold defers
    // to a fill that might already have discharged this obligation, and a fill
    // discharges it through `filled >= owed`, which is already answered.
    if parked_until.is_some_and(|t| now < t) {
        return HedgePlan::Parked;
    }
    // An unattributed fill on this obligation's own venue+market, while an
    // attempt of ITS OWN is still waiting for an `order_ack`, may BE that
    // attempt's fill: hedges are IOC, so they fill in the instant they are
    // accepted and the fill frame can overtake the ack that names it (observed
    // margin 48 ms). Re-placing over it is how one 5-lot hedge becomes 10.
    //
    // `ack_outstanding` is what makes this narrow enough to be worth doing. The
    // predicate used to be venue+market alone, so ONE foreign fill held every
    // obligation in that market — including obligations whose first attempt was
    // refused by the slip gate and which therefore have nothing at the venue at
    // all, where the fill provably cannot be theirs. On a shared account (the
    // Python stack, hand trades) that was up to 59.9s of added naked time on a
    // 60s horizon, bought for nothing.
    //
    // OPERATOR NOTE: `fills_unattributed > 0` therefore means "check for a
    // DUPLICATE HEDGE", not merely "reconcile a foreign fill". This hold closes
    // the 48ms ack RACE; it cannot close a PERMANENTLY lost ack (a place that
    // returns `VenueError::Parse` and still rests — gateway.rs documents this
    // happening live). In that case the fill is never attributable, the hold
    // expires here, and a second full-size hedge goes out with
    // `hedges_overfilled` still reading 0 because the attempt that filled was
    // never credited to anything. Bounded and alarmed, but real.
    if ack_outstanding && since_try < HOLD_FOR_ACK {
        return HedgePlan::HoldForAck;
    }
    let Some(touch) = touch else { return HedgePlan::Wait };
    if !hedge_price_acceptable(cx, book_side, touch, anchor_price, &pol.max_slip) {
        return HedgePlan::Wait;
    }
    // Exactly what is still missing. NOT `attempt.qty - credited`: the attempt's
    // size is already net of everything credited before it was placed, so
    // subtracting the cumulative credit again under-asked on every retry after
    // the second.
    HedgePlan::Retry { qty: owed - filled, price: touch.to_string() }
}

/// May the FIRST hedge attempt go out at `touch`?
///
/// It used to go out unconditionally: every RETRY honoured the slip budget and
/// the initial placement did not, which left a hole the size of the whole edge
/// on the one attempt most likely to be executing into the burst that just
/// gapped the book. The anchor is captured when the MAKER order is placed and a
/// maker quote can rest for hours, so "it was just proven at this price" is not
/// true of the maker path; on take-take leg 2 the anchor is milliseconds old and
/// the gate costs nothing, because if the touch has already left the budget the
/// crossing was not real.
///
/// Ungated ONLY when there is no retry policy at all (bench/replay): there, a
/// refusal would leave the obligation with nothing to carry it forward.
pub(super) fn first_attempt_acceptable(
    cx: &mut Cx,
    pol: Option<&HedgeRetry>,
    book_side: BookSide,
    touch: &str,
    anchor: &str,
) -> bool {
    match pol {
        Some(p) => hedge_price_acceptable(cx, book_side, touch, anchor, &p.max_slip),
        None => true,
    }
}

/// The naked-position alarm: fires ONCE per obligation, on the age of its FIRST
/// attempt, and independently of whether the retry is placing or waiting —
/// because waiting is the policy, and waiting must never be silent.
fn naked_alarm_due(
    first_at: std::time::Instant,
    now: std::time::Instant,
    pol: &HedgeRetry,
    already_alarmed: bool,
) -> bool {
    !already_alarmed
        && now.saturating_duration_since(first_at).as_secs_f64() >= pol.alarm_after_s
}

/// Quote-time hedge anchor for a maker order resting on `rel`'s
/// `market_id`/`side`: the top of the book on the OTHER leg, on the side the
/// hedge would TAKE. A maker bid that fills leaves us long, so the hedge sells
/// into the hedge leg's bid; a maker ask that fills leaves us short, so the
/// hedge lifts the hedge leg's ask. That is the same side selection
/// `Quoter::hedge_has_depth` gates places on, so a place intent always has a
/// live top level here — `HedgeAnchor::side` therefore names the hedge-leg
/// BOOK side whose price is in `HedgeAnchor::price`.
///
/// Captured at PLACE time, never at fill time (burst-gap postmortem: the burst
/// that fills you is the burst that gaps your book).
///
/// NEVER off a crossed book. On an inverted book the touch we would anchor to IS
/// the phantom (`KXRATECUT-26DEC31` sat bid 0.1770 >= ask 0.0730 for a 441-minute
/// unbroken run on 2026-07-28, in a relationship the armed unit's `--rel-prefix`
/// matched), and an anchor minted there is a price the hedge can never reach
/// inside `max_slip` — so the obligation would wait forever instead of hedging.
/// Returning `None` is fail-closed AND loud: the fill path leaves that
/// obligation unconsumed on purpose, which trips `dropped_unconsumed()`. The
/// quoter now refuses to price off a crossed book too, so this is defence in
/// depth and should never be the thing that fires.
pub(super) fn hedge_anchor(
    rel: &Rel,
    market_id: &str,
    side: BookSide,
    books: &BookBuilder,
) -> Option<HedgeAnchor> {
    let i = rel.legs.iter().position(|l| l.market_id == market_id)?;
    let hedge = rel.legs.get(1 - i)?;
    let book = books.get(hedge.venue, &hedge.market_id).filter(|b| !b.is_crossed())?;
    let lvl = match side {
        BookSide::Bid => book.bids.first(),
        BookSide::Ask => book.asks.first(),
    }?;
    Some(HedgeAnchor {
        venue: hedge.venue,
        market_id: hedge.market_id.clone(),
        side,
        price: lvl.price.clone(),
    })
}

/// What ONE hedge deadline tick decides, for every outstanding obligation.
///
/// `hedge_plan` was the first half of the seam this arm needed; this is the
/// rest of it. Which side of which book the touch is read from, whether an
/// unattributed fill can plausibly be this obligation's own, and what the naked
/// alarm says were all still reachable only through a `tokio::select!` arm over
/// live channels until they moved here.
///
/// Returns `(chain id, plan, alarm line)`, sorted by chain id: the dispatch
/// that follows must not depend on `HashMap` iteration order.
fn hedge_tick_plans(
    cx: &mut Cx,
    pol: &HedgeRetry,
    pending: &HashMap<String, PendingHedge>,
    books: &BookBuilder,
    oid_venue: &HashMap<String, String>,
    unclaimed: &HashMap<String, UnclaimedFill>,
    mono: std::time::Instant,
) -> Vec<(String, HedgePlan, Option<String>)> {
    let mut plans: Vec<(String, HedgePlan, Option<String>)> = Vec::new();
    for (chain, p) in pending.iter() {
        // The side of the hedge leg's book we TAKE. Read off the
        // OBLIGATION's anchor, not off the last attempt: the anchor
        // is the one thing about this obligation that never moves.
        let book_side = p.anchor.side;
        let hedge_book = books.get(p.anchor.venue, &p.anchor.market_id);
        // The touch is read even off a CROSSED book, unlike
        // `hedge_anchor`, and the asymmetry is deliberate. Refusing to
        // OPEN an obligation on corrupt data costs nothing; refusing to
        // DISCHARGE one already owed strands real directional exposure
        // to resolution. And a phantom touch cannot make this trade
        // badly: the hedge is an IOC LIMIT, so the phantom is a ceiling
        // on what we pay and a floor on what we receive — the worst it
        // can do is not fill. That rests on the hedge really reaching
        // the wire as an IOC: it does, but via `post_only: false`, not
        // via `PlaceRequest::tif`, which BOTH wire builders ignore
        // (`wire.rs` inlines the TIF from `post_only`, and
        // `Tif::is_maker` documents itself as unused). The two agree at
        // the one call site that builds a hedge (`intent_actions`:
        // `taker` sets `tif: Ioc` AND `post_only: false`). If that ever
        // drifts, this reasoning drifts with it.
        //
        // What the phantom is judged against is the obligation's anchor,
        // which was itself minted off a book proven un-crossed. A
        // crossed hedge book is instead reported in the naked alarm, so
        // an operator learns WHY it will not clear.
        let touch = hedge_book
            .and_then(|b| match book_side {
                BookSide::Bid => b.bids.first(),
                BookSide::Ask => b.asks.first(),
            })
            .map(|l| l.price.clone());
        // Does THIS obligation have an attempt at the venue whose
        // `order_ack` has not landed? Only then can an unattributed
        // fill on its market plausibly be its own. An obligation whose
        // first attempt the slip gate refused has nothing at the venue,
        // so holding it would be added naked time bought for nothing.
        let ack_outstanding = p.latest_attempt.as_ref().is_some_and(|a| !oid_venue.contains_key(a))
            && unclaimed.values().any(|u| {
                u.market_id == p.anchor.market_id && p.anchor.venue == u.venue
            });
        let plan = hedge_plan(
            cx,
            pol,
            p.owed,
            p.filled,
            &p.anchor.price,
            book_side,
            touch.as_deref(),
            p.last_try_at,
            mono,
            ack_outstanding,
            p.parked_until,
        );
        // The alarm is independent of the plan: waiting is the policy
        // (Geoff 2026-07-22, "hedge only if profitable; otherwise find
        // a profitable hedge in the future"), and waiting must never be
        // silent. It survives a PARTIAL fill now — the remainder used
        // to lose both its retry and this alarm.
        let alarm = (!matches!(plan, HedgePlan::Retire)
            && naked_alarm_due(p.first_at, mono, pol, p.alarmed))
        .then(|| {
            let crossed = hedge_book
                .and_then(|b| b.crossing())
                .map(|(bid, ask)| {
                    format!(
                        " — and the hedge leg's book is CROSSED (bid {bid} >= ask \
                         {ask}), so its touch is a phantom"
                    )
                })
                .unwrap_or_default();
            format!(
                "[hedge] NAKED {}x {} on {} for {:.0}s after {} tries — the book has \
                 not offered a price that keeps the basket profitable (anchor {} on \
                 the {} side, budget {}){crossed}",
                p.owed - p.filled,
                p.anchor.market_id,
                p.anchor.venue.as_str(),
                mono.saturating_duration_since(p.first_at).as_secs_f64(),
                p.tries,
                p.anchor.price,
                // The operator reads this line; `{book_side:?}` would spell it
                // `Bid`, and the alarm has always said `bid`.
                book_side.as_str(),
                pol.max_slip,
            )
        });
        plans.push((chain.clone(), plan, alarm));
    }
    plans.sort_by(|a, b| a.0.cmp(&b.0)); // deterministic dispatch order
    plans
}

impl Engine {
    /// The hedge deadline.
    pub(super) fn hedge_tick(&mut self) {
        let pol = self.cfg.hedge_retry.as_ref().expect("guarded above");
        // ONE monotonic reading for the whole tick, so every obligation is
        // judged against the same clock and the decide/act phases cannot
        // disagree. Monotonic and not tape time: tape time freezes exactly
        // when the market feed dies while the fill feed keeps delivering,
        // which pinned both the retry interval and the naked alarm at 0
        // for as long as the feed stayed down (see `PendingHedge::first_at`).
        let mono = std::time::Instant::now();
        // DECIDE first, ACT second. The decision for one obligation is a
        // pure function of the obligation, the book and the clock
        // (`hedge_plan`) — the seam that makes this arm testable at all.
        // It had none, which is why all four of its defects survived.
        let plans = hedge_tick_plans(
            &mut self.cx,
            pol,
            &self.pending_hedges,
            &self.books,
            &self.oid_venue,
            &self.unclaimed_fills,
            mono,
        );
        // WHAT THIS ENGINE IS ALREADY WORKING, published for the one other
        // order-owner inside this process: the venue-truth naked-leg completer
        // (`--positions-recon-act`), which reads venue POSITIONS and so cannot
        // tell an unhedged leg from a leg whose hedge is on the wire.
        //
        // Published from HERE — one wholesale overwrite of the whole set, once a
        // tick — rather than by enter/leave calls at the four `pending_hedges`
        // mutation sites, because those can be got wrong in the fatal direction.
        // A missed leave costs a market this process declines to touch; a missed
        // enter is the 2026-07-30 double hedge. See `naked_act::Inflight` for
        // why 1 Hz is fast enough against a 5-minute cycle.
        //
        // Unconditional, and that matters: an EMPTY set has to be published as
        // eagerly as a full one, or the reader's staleness guard fails closed on
        // an idle engine and no naked leg is ever completed.
        crate::naked_act::publish_inflight(
            self.pending_hedges.values().map(|p| p.anchor.market_id.clone()).collect(),
        );
        self.apply_hedge_plans(plans, mono);
    }

    /// The venue REFUSED a place because the market is halted.
    ///
    /// The one class of refusal the executor reports (`Retry::MarketHalted`);
    /// see the emission site in `exec.rs` for why the other two need no message.
    /// A refusal on anything that is not a live hedge attempt — a maker quote, a
    /// take-take leg 1, an attempt whose obligation is already discharged — is
    /// read and dropped here: this is the hedge retry's backoff, and it is not
    /// licensed to change what any other path does.
    ///
    /// The clock is MONOTONIC and read HERE rather than taken from the message's
    /// `ts_local_ns`, which is the same rule `PendingHedge::first_at` follows and
    /// for the same reason: every other hedge deadline is monotonic, and mixing
    /// the two would let a wall-clock step decide when a park ends.
    pub(super) fn on_place_result(&mut self, v: &serde_json::Value) {
        // Read the class rather than assume it. The field is the contract
        // between the two halves, and an emitter that later reports more
        // classes must not silently start parking on all of them.
        if v.get("retry").and_then(|r| r.as_str()) != Some("market_halted") {
            return;
        }
        let Some(oid) = v.get("order_id").and_then(|x| x.as_str()) else { return };
        let Some(chain) = self.hedge_orders.get(oid).map(|h| h.chain_id.clone()) else { return };
        let now = std::time::Instant::now();
        let Some(p) = self.pending_hedges.get_mut(&chain) else { return };
        p.paused_strikes += 1;
        let park = venue_reopen_park(p.paused_strikes);
        p.parked_until = Some(now + park);
        let (owed, filled, strikes) = (p.owed, p.filled, p.paused_strikes);
        let (market, venue) = (p.anchor.market_id.clone(), p.anchor.venue);
        self.n_parked += 1;
        // ONE line per refusal, which is one line per park and not one per
        // tick. It has to say NAKED out loud: the whole risk of backing off is
        // that a quieter log reads like a solved problem.
        eprintln!(
            "[hedge] PARKED {}x {market} on {} for {}s — the venue refused the place \
             with `trading_is_paused` (strike {strikes}); no price can fill against a \
             halted market. The obligation {chain} is STILL OWED and STILL NAKED, and \
             its alarm is unchanged.",
            owed - filled,
            venue.as_str(),
            park.as_secs(),
        );
    }

    /// The ACT half of the hedge deadline: everything `hedge_tick_plans`
    /// decided, in the order it decided it.
    fn apply_hedge_plans(
        &mut self,
        plans: Vec<(String, HedgePlan, Option<String>)>,
        mono: std::time::Instant,
    ) {
        for (chain, plan, alarm) in plans {
            if let Some(msg) = alarm {
                if let Some(p) = self.pending_hedges.get_mut(&chain) {
                    p.alarmed = true;
                }
                self.n_naked += 1;
                eprintln!("{msg}");
            }
            match plan {
                // Not due, or the book will not offer a profitable price.
                // `last_try_at` is NOT bumped on a wait: it gates
                // PLACEMENTS, and looking at the book again next tick is
                // free, so a naked leg hedges the moment the price comes
                // back instead of up to `interval_s` later.
                //
                // `Parked` joins them for the same reason: the park is held on
                // the obligation and expires on its own clock, so nothing here
                // has to be bumped. Its LINE was already said once, at the
                // moment the venue refused (`on_place_result`) — repeating it
                // every second would be the storm again in a quieter font.
                HedgePlan::Hold | HedgePlan::Wait | HedgePlan::Parked => {}
                // Deferring to a fill we cannot attribute. Said out loud
                // once per obligation: `HedgePlan::Hold` used to swallow
                // this, so added naked time had no signal at all.
                HedgePlan::HoldForAck => {
                    let Some(p) = self.pending_hedges.get_mut(&chain) else { continue };
                    if !p.hold_logged {
                        p.hold_logged = true;
                        eprintln!(
                            "[hedge] HOLD {}x {} on {} — a fill on that market is not \
                             attributable yet and may be this hedge's own (attempt {}, \
                             no order_ack). Not re-placing for up to {}s; if \
                             fills_unattributed rises, CHECK FOR A DUPLICATE HEDGE.",
                            p.owed - p.filled,
                            p.anchor.market_id,
                            p.anchor.venue.as_str(),
                            p.latest_attempt.as_deref().unwrap_or("?"),
                            HOLD_FOR_ACK.as_secs()
                        );
                    }
                }
                HedgePlan::Retire => {
                    // Covered after all — a late fill landed. The attempts
                    // stay in `hedge_orders`, so a further frame on one is
                    // recognised as an over-fill rather than as money we
                    // cannot explain.
                    self.pending_hedges.remove(&chain);
                }
                HedgePlan::Retry { qty, price } => {
                    self.next_hedge_oid += 1;
                    let hoid = format!("h{}", self.next_hedge_oid);
                    let Some(p) = self.pending_hedges.get_mut(&chain) else { continue };
                    p.tries += 1;
                    p.last_try_at = mono;
                    // The attempt this one supersedes, captured BEFORE
                    // `latest_attempt` moves on. Every retry gets a FRESH
                    // client_order_id, so the venue sees two unrelated orders
                    // and would happily fill both — its `409
                    // order_already_exists` cannot save us here.
                    let supersedes = p.latest_attempt.clone();
                    // This attempt becomes the one whose `order_ack` the
                    // ack-hold waits for, and it gets its own hold line:
                    // a new attempt is a new ack that can go missing.
                    p.latest_attempt = Some(hoid.clone());
                    p.hold_logged = false;
                    let (tries, owed, filled) = (p.tries, p.owed, p.filled);
                    let ho = HedgeOrder {
                        maker_order_id: p.maker_order_id.clone(),
                        chain_id: chain.clone(),
                        market_id: p.anchor.market_id.clone(),
                        venue: p.anchor.venue,
                        side: taking_side(p.anchor.side),
                        price: price.clone(),
                        qty,
                        cum_filled: 0,
                        supersedes,
                    };
                    self.n_retry += 1;
                    eprintln!(
                        "[hedge] retry {hoid} {qty}x {} @ {price} (try {tries}; \
                         obligation {chain} owed {owed}, {filled} hedged)",
                        ho.market_id
                    );
                    self.intents.push(Intent::Place(intent::Place {
                        count: ho.qty,
                        old_price: None,
                        order_id: hoid.clone(),
                        place: ho.market_id.clone(),
                        price: ho.price.clone(),
                        replaces: None,
                        retry: Some(tries),
                        side: ho.side,
                        tag: Some(Tag::Hedge),
                        taker: true,
                        ts: self.last_now,
                        venue: ho.venue,
                    }));
                    // EVERY attempt stays in `hedge_orders`, superseded
                    // ones included: an IOC that filled late still has to
                    // credit its obligation, and the obligation's key does
                    // not move, so the credit lands on the right one.
                    self.hedge_orders.insert(hoid, ho);
                    self.drain_intents(Option::<&Rel>::None);
                }
            }
        }
    }
}

/// The hedge DEADLINE, driven end to end through `run()`.
///
/// Everything else about the retry state machine is pinned as a pure function,
/// which is the right seam — but no pure test can prove that the arm reads the
/// clock it is supposed to read. R1 was exactly that class of bug: the policy was
/// correct and the arm fed it tape time. So this drives the real engine over a
/// real channel and then STOPS THE FEED, which is what the market feed dying
/// looks like from in here.
#[cfg(test)]
mod hedge_deadline_tests {
    use super::*;
    // The deadline is this module's; `run` and the shapes it is handed are the
    // parent's.
    use crate::engine::{run, RunCfg};
    use crate::exec::ExecStats;
    use crate::feed::FeedMsg;
    use crate::hist::Hist;
    use arb_core::quoter::Quoter;
    use arb_core::scan::{RelLeg, RelType};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn rel() -> Rel {
        Rel {
            id: "synth-deadline-rel".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "core".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "SYNTH-K-YES".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "SYNTH-P-YES".into() },
            ],
        }
    }

    fn snapshot(venue: &str, market: &str, bid: &str, ask: &str, ts_s: u64) -> String {
        format!(
            r#"{{"kind":"snapshot","venue":"{venue}","market_id":"{market}","seq":1,
                 "ts_local_ns":{},"ts_venue":null,
                 "bids":[{{"price":"{bid}","size":"100"}}],
                 "asks":[{{"price":"{ask}","size":"100"}}]}}"#,
            ts_s as u128 * 1_000_000_000
        )
        .replace('\n', "")
    }

    fn msg(line: String) -> FeedMsg {
        FeedMsg { line, t_read: std::time::Instant::now() }
    }

    /// The engine's own order ids are seeded from the wall clock on a live run, so
    /// a test cannot guess them. Read the first place intent back out of the
    /// `--out` stream, which the engine flushes per line when not benching.
    fn wait_for_first_place(path: &std::path::Path) -> String {
        for _ in 0..200 {
            if let Ok(txt) = std::fs::read_to_string(path) {
                for l in txt.lines() {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(l) else { continue };
                    if v.get("place").is_some() {
                        if let Some(oid) = v.get("order_id").and_then(|x| x.as_str()) {
                            return oid.to_string();
                        }
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        panic!("the quoter never placed anything at {}", path.display());
    }

    /// ...and the same for the HEDGE attempt the maker fill mints. Hedge ids are
    /// the only ones spelled `h<n>` (`Engine::next_hedge_oid`), which is what
    /// tells one apart from the maker place already in this stream.
    fn wait_for_hedge_place(path: &std::path::Path) -> String {
        for _ in 0..200 {
            if let Ok(txt) = std::fs::read_to_string(path) {
                for l in txt.lines() {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(l) else { continue };
                    if v.get("place").is_none() {
                        continue;
                    }
                    match v.get("order_id").and_then(|x| x.as_str()) {
                        Some(oid) if oid.starts_with('h') => return oid.to_string(),
                        _ => {}
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        panic!("the maker fill never produced a hedge place at {}", path.display());
    }

    /// R1, end to end. A maker fill arrives, the market feed then goes silent for
    /// good, and the obligation must still be retried and must still alarm.
    ///
    /// Before this fix both clocks were tape time (`last_now`), which advances only
    /// on a book event, an `order_ack` or a maker fill frame. The fill feed is a
    /// SEPARATE socket, so the fill stamped `first_ts`/`last_try_ts` with its own
    /// event time and then nothing ever advanced `now` again:
    /// `now - last_try_ts == 0 < interval_s` (no retry, ever) and
    /// `now - first_ts == 0 < alarm_after_s` (no alarm, ever). The armed process
    /// was dropped by the feed three times on 2026-07-28. Reproduced by the
    /// reviewer against the real binary as `hedges_pending: 1, hedges_naked: 0,
    /// hedges_retried: 0` across 20s of silence.
    #[tokio::test]
    async fn a_silent_market_feed_still_retries_and_still_alarms() {
        let dir = std::env::temp_dir().join(format!("arb-deadline-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let out = dir.join("intents.jsonl");
        let _ = std::fs::remove_file(&out);

        let q = Quoter::new(rel());
        let mut by_market = HashMap::new();
        by_market.insert((Venue::Kalshi, "SYNTH-K-YES".to_string()), vec![0usize]);
        by_market.insert((Venue::PolymarketUs, "SYNTH-P-YES".to_string()), vec![0usize]);

        let (tx, rx) = mpsc::channel::<FeedMsg>(64);
        let cfg = RunCfg {
            out_path: Some(out.to_string_lossy().to_string()),
            kill_file: dir.join("no-such-kill").to_string_lossy().to_string(),
            stats_every_s: 3600,
            bench: false, // the whole point: the deadlines must run
            wal_path: None,
            health_file: None, // no feed-health pull, so quoting is never suppressed
            toxgate_file: None,
            apr: None,
            apr_installed: (0.0, String::new()),
            risk: None,
            ledger_path: None, // never write the accounting ledger from a test
            hedge_retry: Some(HedgeRetry {
                interval_s: 0.05,
                max_slip: "0.01".into(),
                alarm_after_s: 0.1,
            }),
            take_take: None,
            unwind: None,
            armed: false,
            hedges_undischarged: 0,
        };
        // No executors: nothing can reach a venue from this test by construction.
        let handle = tokio::spawn(run(
            vec![q],
            by_market,
            rx,
            HashMap::new(),
            Arc::new(ExecStats {
                hop: Hist::new(),
                placed: std::sync::atomic::AtomicU64::new(0),
                cancelled: std::sync::atomic::AtomicU64::new(0),
                dropped: std::sync::atomic::AtomicU64::new(0),
                sent: std::sync::atomic::AtomicU64::new(0),
                failed: std::sync::atomic::AtomicU64::new(0),
                recovered: std::sync::atomic::AtomicU64::new(0),
            }),
            cfg,
        ));

        // Two books, so the quoter rests a maker order and an anchor exists.
        tx.send(msg(snapshot("kalshi", "SYNTH-K-YES", "0.30", "0.45", 1_700_000_000)))
            .await
            .expect("send");
        tx.send(msg(snapshot("polymarket_us", "SYNTH-P-YES", "0.40", "0.42", 1_700_000_001)))
            .await
            .expect("send");
        let oid = tokio::task::spawn_blocking({
            let out = out.clone();
            move || wait_for_first_place(&out)
        })
        .await
        .expect("join");

        // The maker fills... and that is the LAST event the engine ever sees. Tape
        // time is frozen at 1700000003 from here on.
        tx.send(msg(format!(
            r#"{{"kind":"fill","venue":"kalshi","market_id":"SYNTH-K-YES","order_id":"{oid}",
                 "cum":5,"ts_local_ns":1700000003000000000}}"#
        )))
        .await
        .expect("send");

        // Wall time passes with no feed at all. The hedge deadline ticks at 1s, so
        // give it a few ticks; every assertion below is a lower bound, so a slow
        // machine cannot make this flake.
        tokio::time::sleep(std::time::Duration::from_millis(3200)).await;
        drop(tx); // feed closed -> run() returns its summary
        let s = handle.await.expect("engine task");

        assert_eq!(s["hedge_obligations"], 1, "the maker fill minted its obligation");
        assert_eq!(s["hedges_pending"], 1, "and it is still open — nothing could fill it");
        assert!(
            s["hedges_naked"].as_u64().expect("hedges_naked") >= 1,
            "the naked alarm MUST fire on wall time, not on the next book event: {s}"
        );
        assert!(
            s["hedges_retried"].as_u64().expect("hedges_retried") >= 1,
            "and the retry MUST become due on wall time: {s}"
        );
        assert_eq!(s["fills_unattributed"], 0, "the fill was ours and was attributed");
        assert_eq!(s["dropped_unconsumed"], 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// THE 2026-07-30 STORM, end to end.
    ///
    /// Kalshi had KXNOBELPEACE-27-MKUL halted from 04:29:06 to 04:59:58 and
    /// answered every place with `409 trading_is_paused`. The engine re-sent the
    /// same 1 lot at the same 0.0800 for 31 minutes — 335 attempts — because a
    /// refused place answered it with nothing, and nothing is what "not filled
    /// yet" looks like too. Every retry was correct policy applied to a fact the
    /// engine did not have.
    ///
    /// Driven through `run()` and not against the pure policy, because the pure
    /// policy was never wrong: the defect was the missing EVENT, and only the
    /// real arm can prove the event now arrives and is acted on. The synthetic
    /// rel's venues are the harness's, not the incident's; the verbatim Kalshi
    /// body is pinned where it is parsed (`arb_venue::error`).
    #[tokio::test]
    async fn a_venue_that_says_the_market_is_halted_stops_the_retry_storm() {
        let dir = std::env::temp_dir().join(format!("arb-halted-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let out = dir.join("intents.jsonl");
        let _ = std::fs::remove_file(&out);

        let q = Quoter::new(rel());
        let mut by_market = HashMap::new();
        by_market.insert((Venue::Kalshi, "SYNTH-K-YES".to_string()), vec![0usize]);
        by_market.insert((Venue::PolymarketUs, "SYNTH-P-YES".to_string()), vec![0usize]);

        let (tx, rx) = mpsc::channel::<FeedMsg>(64);
        let cfg = RunCfg {
            out_path: Some(out.to_string_lossy().to_string()),
            kill_file: dir.join("no-such-kill").to_string_lossy().to_string(),
            stats_every_s: 3600,
            bench: false,
            wal_path: None,
            health_file: None,
            toxgate_file: None,
            apr: None,
            apr_installed: (0.0, String::new()),
            risk: None,
            ledger_path: None,
            // The live shape: retries due far faster than the test's own window,
            // so an unparked obligation cannot help but storm.
            hedge_retry: Some(HedgeRetry {
                interval_s: 0.05,
                max_slip: "0.01".into(),
                alarm_after_s: 0.1,
            }),
            take_take: None,
            unwind: None,
            armed: false,
            hedges_undischarged: 0,
        };
        let handle = tokio::spawn(run(
            vec![q],
            by_market,
            rx,
            HashMap::new(),
            Arc::new(ExecStats {
                hop: Hist::new(),
                placed: std::sync::atomic::AtomicU64::new(0),
                cancelled: std::sync::atomic::AtomicU64::new(0),
                dropped: std::sync::atomic::AtomicU64::new(0),
                sent: std::sync::atomic::AtomicU64::new(0),
                failed: std::sync::atomic::AtomicU64::new(0),
                recovered: std::sync::atomic::AtomicU64::new(0),
            }),
            cfg,
        ));

        tx.send(msg(snapshot("kalshi", "SYNTH-K-YES", "0.30", "0.45", 1_700_000_000)))
            .await
            .expect("send");
        tx.send(msg(snapshot("polymarket_us", "SYNTH-P-YES", "0.40", "0.42", 1_700_000_001)))
            .await
            .expect("send");
        let maker = tokio::task::spawn_blocking({
            let out = out.clone();
            move || wait_for_first_place(&out)
        })
        .await
        .expect("join");

        // The maker fills, which mints the obligation and sends its first hedge.
        tx.send(msg(format!(
            r#"{{"kind":"fill","venue":"kalshi","market_id":"SYNTH-K-YES","order_id":"{maker}",
                 "cum":5,"ts_local_ns":1700000003000000000}}"#
        )))
        .await
        .expect("send");
        let hedge = tokio::task::spawn_blocking({
            let out = out.clone();
            move || wait_for_hedge_place(&out)
        })
        .await
        .expect("join");

        // ...and the venue refuses it: the market is halted. This is the line
        // that did not exist.
        tx.send(msg(
            serde_json::json!({
                "kind": "place_result",
                "venue": "polymarket_us",
                "market_id": "SYNTH-P-YES",
                "order_id": hedge,
                "ok": false,
                "retry": "market_halted",
                "error": "place: HTTP 409: {\"error\":{\"code\":\"trading_is_paused\"}}",
                "ts_local_ns": 1700000003000000000i64,
            })
            .to_string(),
        ))
        .await
        .expect("send");

        // Wall time passes. The deadline ticks at 1Hz and `interval_s` is 0.05,
        // so every tick in here would have produced another place.
        tokio::time::sleep(std::time::Duration::from_millis(3200)).await;
        drop(tx);
        let s = handle.await.expect("engine task");

        assert_eq!(s["hedges_parked"], 1, "the refusal parked the obligation: {s}");
        assert_eq!(
            s["hedges_retried"], 0,
            "and NOT ONE further place went out while the venue was halted — this \
             is the 335: {s}"
        );
        // The two halves of "parked is not quiet".
        assert_eq!(s["hedges_pending"], 1, "the obligation is still owed");
        assert!(
            s["hedges_naked"].as_u64().expect("hedges_naked") >= 1,
            "and it still alarms as NAKED while parked: {s}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod hedge_retry_tests {
    use super::*;

    fn ok(book_side: BookSide, touch: &str, anchor: &str, slip: &str) -> bool {
        let mut cx = Cx::default();
        hedge_price_acceptable(&mut cx, book_side, touch, anchor, slip)
    }

    /// Selling into a bid: a HIGHER bid is better than we expected, always fine.
    #[test]
    fn selling_takes_any_price_at_or_above_the_anchor() {
        assert!(ok(BookSide::Bid, "0.40", "0.40", "0.00"), "exactly the anchor");
        assert!(ok(BookSide::Bid, "0.45", "0.40", "0.00"), "better than the anchor");
    }

    /// ...and gives up at most max_slip of the edge below it.
    #[test]
    fn selling_gives_up_at_most_max_slip() {
        assert!(ok(BookSide::Bid, "0.39", "0.40", "0.01"), "exactly at the tolerance");
        assert!(!ok(BookSide::Bid, "0.38", "0.40", "0.01"), "past it => WAIT, never chase");
    }

    /// Buying from an ask: worse means paying MORE.
    #[test]
    fn buying_gives_up_at_most_max_slip() {
        assert!(ok(BookSide::Ask, "0.40", "0.40", "0.00"));
        assert!(ok(BookSide::Ask, "0.35", "0.40", "0.00"), "cheaper than expected is fine");
        assert!(ok(BookSide::Ask, "0.41", "0.40", "0.01"));
        assert!(!ok(BookSide::Ask, "0.42", "0.40", "0.01"));
    }

    /// Zero tolerance means the anchor is a hard floor/ceiling — the setting
    /// that never gives up a cent of the basket's edge.
    #[test]
    fn zero_slip_refuses_any_worse_price() {
        assert!(!ok(BookSide::Bid, "0.3999", "0.40", "0"));
        assert!(!ok(BookSide::Ask, "0.4001", "0.40", "0"));
    }

    /// The direction must not be symmetric — swapping the side must flip which
    /// way is "worse", or a retry would chase in one direction.
    #[test]
    fn the_two_sides_are_not_symmetric() {
        assert!(ok(BookSide::Bid, "0.50", "0.40", "0.00"), "selling higher is better");
        assert!(!ok(BookSide::Ask, "0.50", "0.40", "0.00"), "buying higher is worse");
    }
}

/// The hedge accounting: what a fill frame means, what a retry may ask for, and
/// what the slip budget is measured against.
///
/// This state machine had ZERO tests, which is why four defects lived in it at
/// once (see `PendingHedge` for the invariant they each broke). The arithmetic
/// is now two pure functions, so each of them can be pinned by the exact
/// scenario the audit described — and each test names the old rule it replaces.
#[cfg(test)]
mod hedge_accounting_tests {
    use super::*;
    // The last section below is about a fill this module cannot attribute, and
    // the one after it about the ledger record a discharged obligation writes —
    // both live next door in `fill`, and both are here because they are what
    // this state machine's arithmetic is FOR.
    use crate::engine::fill::{book_basket, unclaimed_expired, MakerOrder};
    use arb_core::fill::FillLedger;
    use arb_core::model::Level;
    use arb_core::scan::{RelLeg, RelType};

    fn pol(slip: &str) -> HedgeRetry {
        HedgeRetry { interval_s: 5.0, max_slip: slip.into(), alarm_after_s: 60.0 }
    }

    /// A fixed monotonic origin. `Instant` cannot be built from a number, so the
    /// tests take one reading and add to it — the same trick the cancel tests use,
    /// and the only way to exercise these deadlines without sleeping.
    fn t0() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn after(t: std::time::Instant, secs: f64) -> std::time::Instant {
        t + std::time::Duration::from_secs_f64(secs)
    }

    /// A retry that is due, with a book at the anchor and nothing ambiguous
    /// outstanding — so the only variables are the ones each test moves.
    fn plan(pol: &HedgeRetry, owed: i64, filled: i64, anchor: &str, touch: &str) -> HedgePlan {
        let mut cx = Cx::default();
        let t = t0();
        hedge_plan(
            &mut cx, pol, owed, filled, anchor, BookSide::Bid, Some(touch), t,
            after(t, 100.0), false, None,
        )
    }

    // ------------------------------------------------------- I1 / I2: credit ---

    /// THE C3 case. A 10-lot hedge that fills 4 then 10 (cumulative) must credit
    /// and book 10. Multi-frame is the norm, not an edge case: Kalshi sends one
    /// frame per trade (its own feed test walks cum 2 then cum 5 for one order)
    /// and PM-US sends PARTIAL_FILL then FILL.
    ///
    /// The old arm did `hedge_orders.remove(oid)` on the FIRST frame, so frame
    /// two matched nothing, fell through to the maker path, found nothing in the
    /// FillLedger (hedges are deliberately never registered there) and was
    /// dropped. It booked 4, understated exposure forever, and lost the
    /// remainder's retry and its naked alarm.
    #[test]
    fn a_hedge_filling_four_then_six_credits_and_books_ten() {
        let f1 = hedge_credit(4, 10, 0, Some((10, 0)));
        assert_eq!(f1, HedgeCredit { delta: 4, book: 4, over: 0, done: false });
        // the attempt's cum is now 4 and the obligation's filled is now 4
        let f2 = hedge_credit(10, 10, 4, Some((10, 4)));
        assert_eq!(f2, HedgeCredit { delta: 6, book: 6, over: 0, done: true });
        assert_eq!(f1.book + f2.book, 10, "the basket is 10, not 4");
        assert_eq!(f1.over + f2.over, 0);
    }

    /// A frame that tells us nothing new changes nothing — the same cumulative
    /// idempotence `arb_core::fill` gives the maker side, so a poll and a socket
    /// reporting the same hedge fill cannot book it twice.
    #[test]
    fn a_duplicate_or_stale_hedge_frame_is_a_no_op() {
        for cum in [4, 3, 0] {
            let c = hedge_credit(cum, 10, 4, Some((10, 4)));
            assert_eq!(c, HedgeCredit { delta: 0, book: 0, over: 0, done: false }, "cum {cum}");
        }
    }

    /// A venue over-report clamps to the size we actually asked for.
    #[test]
    fn a_hedge_over_report_clamps_to_the_attempts_size() {
        assert_eq!(hedge_credit(14, 10, 0, Some((10, 0))).delta, 10);
    }

    /// Contracts beyond what the obligation owed are NOT booked as a basket:
    /// there is no maker fill to pair them with, so a basket record would invent
    /// one. They are surfaced as `over` for a human instead. This is the late
    /// fill on a superseded IOC — the only way `filled` can pass `owed`.
    #[test]
    fn a_fill_beyond_the_obligation_is_alarmed_not_booked() {
        // obligation owed 10, 8 already hedged; a superseded attempt reports 4
        assert_eq!(
            hedge_credit(4, 10, 0, Some((10, 8))),
            HedgeCredit { delta: 4, book: 2, over: 2, done: true }
        );
        // ...and once the obligation is retired, all of it is over
        assert_eq!(
            hedge_credit(6, 10, 0, None),
            HedgeCredit { delta: 6, book: 0, over: 6, done: false }
        );
    }

    // -------------------------------------------------- I3: retry and alarm ---

    /// The remainder of a partially-filled hedge keeps BOTH its retry and its
    /// naked alarm. The old arm retired the pending entry on the first frame, so
    /// the remainder had neither: no retry would ever ask for it and no alarm
    /// would ever mention it.
    #[test]
    fn the_residual_after_a_partial_hedge_fill_keeps_its_retry_and_its_alarm() {
        let p = pol("0.01");
        assert!(!hedge_credit(4, 10, 0, Some((10, 0))).done, "4 of 10 retires nothing");
        assert_eq!(
            plan(&p, 10, 4, "0.40", "0.40"),
            HedgePlan::Retry { qty: 6, price: "0.40".into() },
            "the retry asks for the 6 that are still naked"
        );
        let t = t0();
        assert!(naked_alarm_due(t, after(t, 60.0), &p, false), "and the alarm is armed");
        // ...including while the book refuses. Waiting is the policy; waiting
        // silently is the bug.
        assert_eq!(plan(&p, 10, 4, "0.40", "0.30"), HedgePlan::Wait);
        assert!(naked_alarm_due(t, after(t, 60.0), &p, false));
        assert!(!naked_alarm_due(t, after(t, 60.0), &p, true), "and it fires exactly once");
        assert!(!naked_alarm_due(t, after(t, 59.9), &p, false), "not before its time");
    }

    /// Two partial fills on ONE maker order are two obligations, and neither may
    /// retire the other.
    ///
    /// The old credit was keyed by the MAKER order id and compared against one
    /// obligation's qty, so hedging obligation A discharged obligation B on
    /// paper. With A=4 and B=3 the old arithmetic asked for -1, read that as
    /// "fully hedged after all", and retired B — silently naked.
    #[test]
    fn two_partial_fills_on_one_maker_order_each_get_hedged() {
        let p = pol("0.01");
        let a = hedge_credit(4, 4, 0, Some((4, 0)));
        assert!(a.done, "obligation A (4) is covered");
        // B is its own obligation with its own credit; A's fill cannot touch it.
        assert_eq!(
            plan(&p, 3, 0, "0.40", "0.40"),
            HedgePlan::Retry { qty: 3, price: "0.40".into() },
            "obligation B still owes its full 3"
        );
        // the old rule, written out so the regression is unmistakable
        let shared_credit_on_the_maker_order = 4;
        assert!(
            3 - shared_credit_on_the_maker_order <= 0,
            "the old arithmetic made B look already hedged"
        );
    }

    /// The retry asks for exactly what is missing, on the third attempt as much
    /// as the first. `missing = attempt.qty - credited` subtracted a CUMULATIVE
    /// credit from a remainder that was already net of it, so from the second
    /// retry on it under-asked — and once the subtraction went to zero it
    /// retired the obligation with contracts still naked.
    #[test]
    fn the_third_retry_asks_for_exactly_what_is_still_missing() {
        let p = pol("0.05");
        let mut filled = 0;
        // attempt 1 (10 lots) fills 4
        filled += hedge_credit(4, 10, 0, Some((10, filled))).delta;
        assert_eq!(
            plan(&p, 10, filled, "0.40", "0.40"),
            HedgePlan::Retry { qty: 6, price: "0.40".into() },
            "attempt 2 asks for 6"
        );
        // attempt 2 (6 lots) fills 2
        filled += hedge_credit(2, 6, 0, Some((10, filled))).delta;
        assert_eq!(
            plan(&p, 10, filled, "0.40", "0.40"),
            HedgePlan::Retry { qty: 4, price: "0.40".into() },
            "attempt 3 asks for the 4 that are left"
        );
        assert_eq!(
            6 - filled, 0,
            "the old arithmetic asked for 0 here, retired the obligation, and left 4 naked"
        );
    }

    /// Covered means retired, whatever the book is doing — and a not-yet-due
    /// obligation does nothing rather than something.
    #[test]
    fn a_covered_obligation_retires_and_an_undue_one_holds() {
        let p = pol("0.01");
        let mut cx = Cx::default();
        assert_eq!(plan(&p, 5, 5, "0.40", "0.10"), HedgePlan::Retire);
        assert_eq!(plan(&p, 5, 9, "0.40", "0.40"), HedgePlan::Retire);
        let t = t0();
        assert_eq!(
            hedge_plan(

                &mut cx, &p, 5, 0, "0.40", BookSide::Bid, Some("0.40"), t, after(t, 4.9), false, None,

            ),
            HedgePlan::Hold,
            "interval_s gates PLACEMENTS"
        );
        // no level on the side we take is a WAIT, not a silent skip
        assert_eq!(
            hedge_plan(&mut cx, &p, 5, 0, "0.40", BookSide::Bid, None, t, after(t, 100.0), false, None),
            HedgePlan::Wait
        );
    }

    // ------------------------------------------------------- I5: the anchor ---

    /// THE C8 case. Walk a book that ticks away from us in half-cent steps, once
    /// with the slip measured against the OBLIGATION's anchor and once with the
    /// anchor re-set to each accepted price — which is what the old arm did,
    /// because it passed `p.hedge.price` (the last attempt's) and then
    /// overwrote it.
    ///
    /// The fixed rule surrenders the budget once and then waits. The ratcheting
    /// rule surrenders the budget again on every retry, forever; the audit
    /// measured ~12c given up on a 1c policy before the 60s alarm.
    #[test]
    fn the_slip_budget_is_measured_against_the_original_anchor() {
        const LADDER: [&str; 12] = [
            "0.3950", "0.3900", "0.3850", "0.3800", "0.3750", "0.3700", "0.3650", "0.3600",
            "0.3550", "0.3500", "0.3450", "0.3400",
        ];
        /// Returns (attempts allowed, worst price accepted).
        fn walk(ratchet: bool) -> (usize, String) {
            let mut cx = Cx::default();
            let p = pol("0.01");
            let mut against = "0.4000".to_string();
            let (mut taken, mut worst) = (0usize, "0.4000".to_string());
            for (i, touch) in LADDER.iter().enumerate() {
                let t = t0();
                let got = hedge_plan(
                    &mut cx,
                    &p,
                    10,
                    0,
                    &against,
                    BookSide::Bid,
                    Some(touch),
                    t,
                    after(t, 100.0 + i as f64 * 10.0),
                    false,
                    None,
                );
                if let HedgePlan::Retry { price, .. } = got {
                    taken += 1;
                    worst = price.clone();
                    if ratchet {
                        against = price; // THE defect
                    }
                }
            }
            (taken, worst)
        }

        assert_eq!(
            walk(false),
            (2, "0.3900".to_string()),
            "against the original anchor: 0.3950 and 0.3900 are inside the 1c budget, and \
             0.3850 onwards is a WAIT — the total given up is the 1c that was authorised"
        );
        assert_eq!(
            walk(true),
            (12, "0.3400".to_string()),
            "re-anchoring to the last attempt accepts every tick: 6c given up in 12 ticks on a \
             1c policy, and it never stops"
        );
    }

    /// The FIRST attempt is gated too. Every retry honoured the budget and the
    /// initial placement did not, so `max_slip` was cosmetic: the worst price
    /// this engine could ever pay was paid on attempt 1, unchecked, and the
    /// anchor it should have been judged against is captured when the MAKER
    /// order is placed — which can be hours before it fills.
    #[test]
    fn the_first_hedge_attempt_honours_the_slip_budget_too() {
        let mut cx = Cx::default();
        let p = pol("0.01");
        // selling into a bid: the touch may be at most 1c below the anchor
        assert!(first_attempt_acceptable(&mut cx, Some(&p), BookSide::Bid, "0.39", "0.40"));
        assert!(
            !first_attempt_acceptable(&mut cx, Some(&p), BookSide::Bid, "0.36", "0.40"),
            "a 4c move against us between place and fill is not a hedge, it is a loss"
        );
        // buying from an ask: worse is paying more
        assert!(first_attempt_acceptable(&mut cx, Some(&p), BookSide::Ask, "0.41", "0.40"));
        assert!(!first_attempt_acceptable(&mut cx, Some(&p), BookSide::Ask, "0.45", "0.40"));
        // ...and with NO retry policy there is nothing to carry a refusal
        // forward, so bench/replay fires unconditionally and stays byte-exact.
        assert!(
            first_attempt_acceptable(&mut cx, None, BookSide::Bid, "0.36", "0.40"),
            "no policy means no gate — a refusal here would drop the obligation"
        );
    }

    /// The anchor is also where the SIDE comes from, and the two sides must stay
    /// asymmetric all the way through the plan.
    #[test]
    fn the_budget_runs_the_right_way_on_each_side() {
        let mut cx = Cx::default();
        let p = pol("0.01");
        let t = t0();
        // buying: worse is paying more
        assert_eq!(
            hedge_plan(

                &mut cx, &p, 5, 0, "0.40", BookSide::Ask, Some("0.41"), t, after(t, 100.0), false, None,

            ),
            HedgePlan::Retry { qty: 5, price: "0.41".into() }
        );
        assert_eq!(
            hedge_plan(

                &mut cx, &p, 5, 0, "0.40", BookSide::Ask, Some("0.42"), t, after(t, 100.0), false, None,

            ),
            HedgePlan::Wait
        );
        // selling: worse is receiving less
        assert_eq!(plan(&p, 5, 0, "0.40", "0.39"), HedgePlan::Retry {
            qty: 5,
            price: "0.39".into()
        });
        assert_eq!(plan(&p, 5, 0, "0.40", "0.38"), HedgePlan::Wait);
        assert_eq!(taking_side(BookSide::Bid), BookSide::Ask, "taking a bid SELLS");
        assert_eq!(taking_side(BookSide::Ask), BookSide::Bid, "taking an ask BUYS");
    }

    // ------------------------------------------- a fill we cannot attribute ---

    /// A fill whose id maps to nothing must not be counted and forgotten.
    /// `arb_core::fill` now SAYS it is foreign rather than answering the same
    /// `None` it gives for a fill it has already hedged, and the engine holds it
    /// for the ack that might name it before alarming.
    ///
    /// Reachable without anything exotic: a place can return `VenueError::Parse`
    /// and still rest (gateway.rs documents it happening live), no `order_ack`
    /// is ever emitted, and the order then fills under the venue's own id.
    #[test]
    fn a_fill_for_an_unknown_order_id_is_held_and_then_alarms() {
        let mut fl = FillLedger::new();
        fl.register_order("m1", "SYNTH-K-YES", 5, None);
        assert!(
            matches!(
                fl.observe_cum_fill("BH9NNPGFA9SX", 3),
                arb_core::fill::FillOutcome::Unknown
            ),
            "a venue id we hold no mapping for is FOREIGN, not 'already hedged'"
        );

        let t = std::time::Instant::now();
        let mut park = HashMap::new();
        park.insert(
            "BH9NNPGFA9SX".to_string(),
            UnclaimedFill {
                venue: Venue::Kalshi,
                market_id: "SYNTH-K-YES".into(),
                cum: 3,
                since: t,
            },
        );
        assert!(
            unclaimed_expired(&park, t + FILL_ACK_GRACE - std::time::Duration::from_millis(1))
                .is_empty(),
            "the ack may still be in flight — do not cry wolf inside the grace"
        );
        assert_eq!(
            unclaimed_expired(&park, t + FILL_ACK_GRACE),
            vec!["BH9NNPGFA9SX".to_string()],
            "past the grace it is unexplained money and must be said out loud"
        );
        // a clock reading from before the park must not panic or expire it
        assert!(unclaimed_expired(&park, t - std::time::Duration::from_secs(3600)).is_empty());
    }

    /// A fill that beats its own `order_ack` must not be hedged twice. Hedges are
    /// IOC, so they fill in the instant they are accepted and the fill frame can
    /// overtake the ack that names it (observed margin 48 ms). While such a fill
    /// is unattributed on this hedge's own market AND an attempt of this
    /// obligation is still waiting for its ack, the retry holds — otherwise a
    /// 5-lot hedge becomes 10 Kalshi long against 5 PM short.
    #[test]
    fn a_hedge_is_not_re_placed_over_a_fill_that_beat_its_ack() {
        let mut cx = Cx::default();
        let p = pol("0.01");
        let t = t0();
        // placed 10s ago, retry due, nothing ambiguous
        assert_eq!(
            hedge_plan(

                &mut cx, &p, 5, 0, "0.40", BookSide::Bid, Some("0.40"), t, after(t, 10.0), false, None,

            ),
            HedgePlan::Retry { qty: 5, price: "0.40".into() },
            "with nothing outstanding this retry is due and priced fine"
        );
        assert_eq!(
            hedge_plan(

                &mut cx, &p, 5, 0, "0.40", BookSide::Bid, Some("0.40"), t, after(t, 10.0), true,
                None,

            ),
            HedgePlan::HoldForAck,
            "an unattributed fill on this market may BE this hedge — do not re-place over it"
        );
    }

    /// The hold is bounded by `HOLD_FOR_ACK`, measured from the PLACEMENT of the
    /// attempt whose ack is missing — the only clock that answers "could that ack
    /// still be coming?". Past it we hedge anyway: a chatty foreign order in the
    /// same market would otherwise start a fresh grace forever and starve the
    /// hedge.
    ///
    /// And the bound is INDEPENDENT of `alarm_after_s`. It used to be
    /// `max(alarm_after_s, FILL_ACK_GRACE)`, so raising `--hedge-alarm-s` to quiet
    /// the logs silently bought a proportionally longer hold on every naked leg.
    #[test]
    fn the_ack_hold_is_bounded_and_independent_of_the_alarm_knob() {
        let mut cx = Cx::default();
        let t = t0();
        let due = HOLD_FOR_ACK.as_secs_f64();
        for alarm_after_s in [1.0, 60.0, 600.0] {
            let p = HedgeRetry { alarm_after_s, ..pol("0.01") };
            assert_eq!(
                hedge_plan(
                    &mut cx,
                    &p,
                    5,
                    0,
                    "0.40",
                    BookSide::Bid,
                    Some("0.40"),
                    t,
                    after(t, due - 0.1),
                    true,
                    None,
                ),
                HedgePlan::HoldForAck,
                "inside the grace the ack may still land (alarm {alarm_after_s})"
            );
            assert_eq!(
                hedge_plan(
                    &mut cx, &p, 5, 0, "0.40", BookSide::Bid, Some("0.40"), t, after(t, due),
                    true, None,
                ),
                HedgePlan::Retry { qty: 5, price: "0.40".into() },
                "past the grace, being naked is the larger harm (alarm {alarm_after_s})"
            );
        }
    }

    /// R1 — THE FROZEN-FEED CASE. The retry interval and the naked alarm are
    /// measured on a MONOTONIC clock, so neither can be stopped by a dead market
    /// feed.
    ///
    /// Both used to run on tape time (`last_now`), which advances only on a book
    /// event, an `order_ack`, or a maker fill frame. The fill feed is a SEPARATE
    /// socket: a maker fill stamped the obligation's clocks from its own event
    /// time, and if the market feed then died, `now - last_try_ts` and
    /// `now - first_ts` both stayed pinned at 0 — no retry ever became due and the
    /// alarm could never fire, for as long as the feed stayed down. The armed
    /// process was dropped by the feed three times on 2026-07-28 (13:20:10,
    /// 13:28:41, 13:56:15), and the first-attempt slip gate makes it strictly
    /// worse: attempt 1 can now be refused, so a frozen clock means never retried
    /// AND never alarmed.
    #[test]
    fn a_dead_market_feed_stops_neither_the_retry_nor_the_naked_alarm() {
        let mut cx = Cx::default();
        let p = pol("0.01");
        let fill_at = t0(); // tape time stops HERE and never advances again
        // 30s of wall time later, with not one book event in between:
        let plan = hedge_plan(
            &mut cx,
            &p,
            5,
            0,
            "0.40",
            BookSide::Bid,
            Some("0.40"),
            fill_at,
            after(fill_at, 30.0),
            false,
            None,
        );
        assert_eq!(
            plan,
            HedgePlan::Retry { qty: 5, price: "0.40".into() },
            "the retry must become due on wall time, not on the next book event"
        );
        // ...and the alarm on the same clock, so `hedges_naked` cannot read 0 for
        // as long as the feed is down while we hold naked exposure.
        assert!(!naked_alarm_due(fill_at, after(fill_at, 59.9), &p, false));
        assert!(
            naked_alarm_due(fill_at, after(fill_at, 60.0), &p, false),
            "60s naked is 60s naked whether or not the tape moved"
        );
        // A monotonic clock also cannot be walked backwards by an NTP step, which
        // a wall-clock or tape-max() form could not promise.
        assert!(!naked_alarm_due(after(fill_at, 3600.0), fill_at, &p, false));
        assert_eq!(
            hedge_plan(
                &mut cx,
                &p,
                5,
                0,
                "0.40",
                BookSide::Bid,
                Some("0.40"),
                after(fill_at, 3600.0),
                fill_at,
                false,
                None,
            ),
            HedgePlan::Hold,
            "a reading from before the stamp must not panic or fire"
        );
    }

    // ------------------------------------------------- C5: the crossed book ---

    fn synth_rel() -> Rel {
        Rel {
            id: "xvus-fedcut-26-usfed-2026-cut".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "KXRATECUT-26DEC31".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        }
    }

    /// No anchor off a crossed hedge book. `KXRATECUT-26DEC31` was inverted (bid
    /// 0.1770 >= ask 0.0730) for a 441-minute unbroken run on 2026-07-28, in a
    /// relationship the armed unit's `--rel-prefix` matched. Anchoring to the
    /// phantom would set a price the hedge can never reach inside `max_slip`, so
    /// the obligation would wait forever; `None` instead leaves the obligation
    /// unconsumed, which trips `dropped_unconsumed()` — fail-closed AND loud.
    #[test]
    fn no_hedge_anchor_off_a_crossed_hedge_book() {
        let rel = synth_rel();
        let mut books = BookBuilder::new();
        // the PM leg is the maker leg here; Kalshi is the hedge leg, inverted
        books.apply_snapshot(
            Venue::PolymarketUs,
            "P",
            vec![Level { price: "0.20".into(), size: "50".into() }],
            vec![Level { price: "0.22".into(), size: "50".into() }],
            1,
            0,
            None,
        );
        books.apply_snapshot(
            Venue::Kalshi,
            "KXRATECUT-26DEC31",
            vec![Level { price: "0.1770".into(), size: "305".into() }],
            vec![Level { price: "0.0730".into(), size: "26".into() }],
            1,
            0,
            None,
        );
        assert!(
            hedge_anchor(&rel, "P", BookSide::Ask, &books).is_none(),
            "an inverted hedge book must not mint an anchor"
        );
        // ...and the same book, un-crossed, anchors normally
        books.apply_snapshot(
            Venue::Kalshi,
            "KXRATECUT-26DEC31",
            vec![Level { price: "0.1760".into(), size: "305".into() }],
            vec![Level { price: "0.1820".into(), size: "26".into() }],
            2,
            0,
            None,
        );
        let a = hedge_anchor(&rel, "P", BookSide::Ask, &books).expect("a sane book anchors");
        assert_eq!(a.price, "0.1820");
        assert_eq!(a.side, BookSide::Ask);
    }

    /// The ledger side of I2, through the real writer: two credits for one
    /// obligation must read back as ONE basket's worth of exposure.
    #[test]
    fn a_partially_filled_hedge_books_its_full_size_across_frames() {
        let dir = std::env::temp_dir().join(format!("arb-partial-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.jsonl");
        let p = path.to_str().unwrap();
        let _ = std::fs::remove_file(p);

        let maker = MakerOrder {
            rel_id: "synth-fill-rel".into(),
            class: "cross-venue-equivalent",
            venue: "kalshi".into(),
            market_id: "SYNTH-K-YES".into(),
            side: BookSide::Bid,
            price: "0.31".into(),
            strategy: "maker-hedge",
        };
        let hedge = HedgeOrder {
            maker_order_id: "m1".into(),
            chain_id: "h1".into(),
            market_id: "SYNTH-P-YES".into(),
            venue: Venue::PolymarketUs,
            side: BookSide::Ask,
            price: "0.40".into(),
            qty: 10,
            cum_filled: 0,
            supersedes: None,
        };
        let f1 = hedge_credit(4, 10, 0, Some((10, 0)));
        let f2 = hedge_credit(10, 10, 4, Some((10, 4)));
        book_basket(p, &maker, &hedge, "h1", f1.book, 1_700_000_000.0);
        book_basket(p, &maker, &hedge, "h1", f2.book, 1_700_000_001.0);

        let open = crate::ledger::open_exposure(crate::ledger::read(p).unwrap());
        assert_eq!(
            open.get("synth-fill-rel"),
            Some(&10.0),
            "a 10-lot hedge that filled 4 then 6 is 10 contracts of exposure, not 4"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The hedge TICK, as opposed to the hedge POLICY.
///
/// `hedge_plan` has been a pure function since the audit, and the tests above
/// pin it exhaustively. Everything AROUND it was still reachable only through a
/// `tokio::select!` arm over live channels: which side of which book the touch
/// is read from, whether an unattributed fill can plausibly be this
/// obligation's own, what the naked alarm says, and what the act half does to
/// the obligation afterwards. Those are `hedge_tick_plans` and
/// `Engine::apply_hedge_plans` now, and this is them.
#[cfg(test)]
mod hedge_tick_tests {
    use super::*;
    use crate::engine::{test_cfg, test_engine, RunCfg};
    use arb_core::model::Level;

    fn pol() -> HedgeRetry {
        HedgeRetry { interval_s: 5.0, max_slip: "0.01".into(), alarm_after_s: 60.0 }
    }

    fn t0() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn after(t: std::time::Instant, secs: f64) -> std::time::Instant {
        t + std::time::Duration::from_secs_f64(secs)
    }

    /// An obligation for 5 lots on the PM-US bid, placed at `at`. `attempt` is
    /// `None` when the slip gate refused the first attempt — the case that has
    /// NOTHING at the venue.
    fn pending(attempt: Option<&str>, at: std::time::Instant) -> PendingHedge {
        PendingHedge {
            maker_order_id: "m1".into(),
            owed: 5,
            filled: 0,
            anchor: HedgeAnchor {
                venue: Venue::PolymarketUs,
                market_id: "P".into(),
                side: BookSide::Bid,
                price: "0.40".into(),
            },
            first_at: at,
            last_try_at: at,
            latest_attempt: attempt.map(str::to_string),
            tries: u32::from(attempt.is_some()),
            alarmed: false,
            hold_logged: false,
            parked_until: None,
            paused_strikes: 0,
        }
    }

    fn books(bid: &str, ask: &str) -> BookBuilder {
        let mut b = BookBuilder::new();
        b.apply_snapshot(
            Venue::PolymarketUs,
            "P",
            vec![Level { price: bid.into(), size: "50".into() }],
            vec![Level { price: ask.into(), size: "50".into() }],
            1,
            0,
            None,
        );
        b
    }

    /// One fill on `market` that no order of ours claims.
    fn unclaimed(market: &str, at: std::time::Instant) -> HashMap<String, UnclaimedFill> {
        HashMap::from([(
            "BH9NNPGFA9SX".to_string(),
            UnclaimedFill {
                venue: Venue::PolymarketUs,
                market_id: market.into(),
                cum: 5,
                since: at,
            },
        )])
    }

    /// THE narrowing that makes the ack-hold worth having. A fill we cannot
    /// attribute on this obligation's own market holds its retry ONLY while an
    /// attempt of its own is still waiting for an `order_ack`.
    ///
    /// The predicate used to be venue+market alone, so one foreign fill held
    /// every obligation in that market — including obligations whose first
    /// attempt the slip gate refused, which have nothing at the venue at all and
    /// where the fill provably cannot be theirs. On a shared account (the Python
    /// stack, hand trades) that was up to 59.9s of added naked time on a 60s
    /// horizon, bought for nothing.
    #[test]
    fn only_an_obligation_with_an_unacked_attempt_holds_for_a_foreign_fill() {
        let mut cx = Cx::default();
        let (p, t) = (pol(), t0());
        let due = after(t, 6.0);
        let bk = books("0.40", "0.41");
        let park = unclaimed("P", t);
        let no_acks = HashMap::new();

        // an attempt of ours is out and its ack has not landed: this fill may be
        // that attempt's own, so do not re-place over it
        let mut pend = HashMap::from([("h1".to_string(), pending(Some("h1"), t))]);
        let plans = hedge_tick_plans(&mut cx, &p, &pend, &bk, &no_acks, &park, due);
        assert_eq!(plans[0].1, HedgePlan::HoldForAck);

        // the SAME fill, but this obligation's first attempt was refused, so it
        // has nothing at the venue that could have produced it
        pend.insert("h1".to_string(), pending(None, t));
        let plans = hedge_tick_plans(&mut cx, &p, &pend, &bk, &no_acks, &park, due);
        assert_eq!(
            plans[0].1,
            HedgePlan::Retry { qty: 5, price: "0.40".into() },
            "an obligation with nothing at the venue must not wait on a foreign fill"
        );

        // ...and once the attempt's ack HAS landed, the fill is not its own
        // either: we would have recognised it.
        pend.insert("h1".to_string(), pending(Some("h1"), t));
        let acked = HashMap::from([("h1".to_string(), "venue-side-id".to_string())]);
        let plans = hedge_tick_plans(&mut cx, &p, &pend, &bk, &acked, &park, due);
        assert_eq!(plans[0].1, HedgePlan::Retry { qty: 5, price: "0.40".into() });

        // ...and a fill on a DIFFERENT market was never ambiguous at all
        pend.insert("h1".to_string(), pending(Some("h1"), t));
        let elsewhere = unclaimed("SOME-OTHER-MARKET", t);
        let plans = hedge_tick_plans(&mut cx, &p, &pend, &bk, &no_acks, &elsewhere, due);
        assert_eq!(plans[0].1, HedgePlan::Retry { qty: 5, price: "0.40".into() });
    }

    /// The naked alarm has to say WHY the leg will not clear. A crossed hedge
    /// book is honoured for the touch — refusing to DISCHARGE an obligation on
    /// corrupt data strands real directional exposure to resolution — but it is
    /// REPORTED, so an operator learns that the price being waited on is a
    /// phantom. `KXRATECUT-26DEC31` sat inverted for a 441-minute unbroken run
    /// on 2026-07-28.
    #[test]
    fn the_naked_alarm_reports_a_crossed_hedge_book() {
        let mut cx = Cx::default();
        let t = t0();
        let bk = books("0.50", "0.40"); // bid >= ask: OUR book is corrupt
        let pend = HashMap::from([("h1".to_string(), pending(Some("h1"), t))]);
        let plans = hedge_tick_plans(
            &mut cx,
            &pol(),
            &pend,
            &bk,
            &HashMap::new(),
            &HashMap::new(),
            after(t, 61.0),
        );
        let alarm = plans[0].2.as_deref().expect("61s naked on a 60s horizon must alarm");
        assert!(alarm.contains("NAKED 5x P on polymarket_us"), "{alarm}");
        assert!(alarm.contains("after 1 tries"), "{alarm}");
        assert!(alarm.contains("anchor 0.40 on the bid side, budget 0.01"), "{alarm}");
        assert!(alarm.contains("CROSSED (bid 0.50 >= ask 0.40)"), "{alarm}");
        assert!(alarm.contains("phantom"), "{alarm}");

        // ...and a sane book alarms without the crossed clause
        let sane = books("0.40", "0.41");
        let pend = HashMap::from([("h1".to_string(), pending(Some("h1"), t))]);
        let plans = hedge_tick_plans(
            &mut cx,
            &pol(),
            &pend,
            &sane,
            &HashMap::new(),
            &HashMap::new(),
            after(t, 61.0),
        );
        let alarm = plans[0].2.as_deref().expect("still naked, still alarmed");
        assert!(!alarm.contains("CROSSED"), "{alarm}");
    }

    /// THE INTERLOCK THE OTHER ORDER-OWNER READS. `--positions-recon-act`
    /// completes naked legs from venue POSITIONS, which cannot tell an unhedged
    /// leg from one whose hedge is on the wire — so the hedge tick has to say
    /// what it is working, every tick, or the two buy the same contract twice
    /// (2026-07-30, and `hedges_overfilled` read 0 through it).
    ///
    /// Both directions are asserted because both are load-bearing: an obligation
    /// must APPEAR, and an idle engine must publish an EMPTY set rather than
    /// stay silent — the reader fails closed on silence, so a hedge tick that
    /// only spoke when it had something to say would mean no naked leg is ever
    /// completed.
    #[tokio::test]
    async fn the_hedge_tick_publishes_what_this_engine_is_already_working() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        crate::naked_act::reset_inflight();
        let mut e = test_engine(RunCfg { hedge_retry: Some(pol()), ..test_cfg() });

        // Idle: the tick still speaks, and everything is placeable.
        e.hedge_tick();
        assert!(crate::naked_act::inflight_check("KXTEST").is_ok());

        // With an obligation on KXTEST, that market becomes untouchable...
        e.pending_hedges.insert("h1".into(), pending(Some("h1"), t0()));
        e.hedge_tick();
        let err = crate::naked_act::inflight_check(&e.pending_hedges["h1"].anchor.market_id)
            .expect_err("this engine owes a hedge there");
        assert!(err.contains("double hedge"), "{err}");
        // ...and nothing else is.
        assert!(crate::naked_act::inflight_check("KXSOMETHINGELSE").is_ok());

        // ...and when it is discharged the market is released, because the set
        // is republished WHOLESALE rather than maintained by paired calls.
        e.pending_hedges.clear();
        e.hedge_tick();
        assert!(crate::naked_act::inflight_check("KXTEST").is_ok());
    }

    /// Dispatch order is by chain id and nothing else. `pending_hedges` is a
    /// `HashMap`, so without the sort the same tape would act on the same
    /// retries in a different order on every run — and this arm PLACES.
    #[test]
    fn plans_come_back_in_chain_id_order_not_hash_order() {
        let mut cx = Cx::default();
        let t = t0();
        let pend: HashMap<String, PendingHedge> = ["h3", "h1", "h2", "h10"]
            .iter()
            .map(|id| (id.to_string(), pending(Some(id), t)))
            .collect();
        let plans = hedge_tick_plans(
            &mut cx,
            &pol(),
            &pend,
            &books("0.40", "0.41"),
            &HashMap::new(),
            &HashMap::new(),
            after(t, 6.0),
        );
        let ids: Vec<&str> = plans.iter().map(|(c, _, _)| c.as_str()).collect();
        assert_eq!(ids, ["h1", "h10", "h2", "h3"], "byte order, deterministically");
    }

    /// The ACT half. A retry mints a NEW attempt id, but the OBLIGATION's key —
    /// its chain id — does not move: that is what makes a late fill on any
    /// attempt in the chain credit the right obligation. It re-arms the ack hold
    /// too, because a new attempt is a new ack that can go missing.
    #[test]
    fn a_retry_mints_a_new_attempt_under_the_same_chain_id() {
        let mut e = test_engine(RunCfg { hedge_retry: Some(pol()), ..test_cfg() });
        let t = t0();
        // attempt h1 is already out, and its hold has already been logged
        e.next_hedge_oid = 1;
        e.pending_hedges
            .insert("h1".into(), PendingHedge { hold_logged: true, ..pending(Some("h1"), t) });

        let mono = after(t, 6.0);
        e.apply_hedge_plans(
            vec![("h1".into(), HedgePlan::Retry { qty: 5, price: "0.40".into() }, None)],
            mono,
        );

        let p = &e.pending_hedges["h1"];
        assert_eq!(p.tries, 2);
        assert_eq!(p.latest_attempt.as_deref(), Some("h2"), "a new attempt");
        // ...and the kill sweep can claim it. This id was MINTED by the code
        // under test, not written here, so it pins the `h` counter's format
        // against `arb_venue::gateway::is_ours` end to end — a hedge id the
        // sweep cannot recognise is a hedge the sweep silently stops cancelling.
        assert!(
            arb_venue::gateway::is_ours(p.latest_attempt.as_deref().unwrap()),
            "a minted hedge id must be sweepable: {:?}",
            p.latest_attempt
        );
        assert_eq!(p.last_try_at, mono, "and `interval_s` runs from this placement");
        assert!(!p.hold_logged, "a new attempt is a new ack that can go missing");
        assert_eq!((p.owed, p.filled), (5, 0), "the obligation itself is unchanged");

        let ho = &e.hedge_orders["h2"];
        assert_eq!(ho.chain_id, "h1", "the obligation's name does not move across retries");
        assert_eq!((ho.qty, ho.cum_filled), (5, 0));
        assert_eq!(ho.side, BookSide::Ask, "taking a bid SELLS");
        assert_eq!(e.n_retry, 1);
        // ...and it records WHICH attempt it supersedes, captured before
        // `latest_attempt` moved on. That is the id the executor asks the venue
        // about before this retry is allowed on the wire: h1 is an IOC that may
        // have filled with its fill frame lost, and re-placing over it is how
        // one 5-lot hedge becomes 10 — long against our own short, with
        // `hedges_overfilled` still reading 0 because the second fill is
        // credited to an obligation the first already discharged.
        assert_eq!(
            ho.supersedes.as_deref(),
            Some("h1"),
            "the retry must name the attempt whose fate is unknown"
        );
        // A THIRD attempt supersedes the second, not the first: each attempt is
        // verified before the next goes out, so the chain stays covered.
        e.apply_hedge_plans(
            vec![("h1".into(), HedgePlan::Retry { qty: 5, price: "0.40".into() }, None)],
            after(t, 12.0),
        );
        assert_eq!(e.hedge_orders["h3"].supersedes.as_deref(), Some("h2"));

        // ...and a Retire really retires, while leaving the attempts behind
        e.apply_hedge_plans(vec![("h1".into(), HedgePlan::Retire, None)], mono);
        assert!(e.pending_hedges.is_empty());
        assert!(
            e.hedge_orders.contains_key("h2"),
            "a late frame on a retired obligation's attempt must stay recognisable"
        );
    }

    /// THE ONE-CYCLE-LATER DEFECT. A retry the executor WITHHELD never reached
    /// the venue, so it never gets an ack and never enters `oid_venue` — and the
    /// next retry must therefore verify the attempt BEFORE it, not that one.
    ///
    /// `latest_attempt` advances the moment a retry is decided, before the
    /// executor can withhold it, so the chain accumulates links that exist only
    /// in this process. Resolving only the first link would hand the executor
    /// `None` for h3, the verification guard would not match, and the place
    /// would go out with no read at all — on top of an h1 whose fate is exactly
    /// as unknown as it was one interval ago. That buys a retry interval and
    /// then produces the same double hedge.
    #[test]
    fn a_refused_retry_does_not_become_the_attempt_the_next_retry_verifies() {
        let mut e = test_engine(RunCfg { hedge_retry: Some(pol()), ..test_cfg() });
        let t = t0();
        e.next_hedge_oid = 1;
        e.pending_hedges.insert("h1".into(), pending(Some("h1"), t));
        // h1 is the chain's FIRST attempt, minted on the maker fill — so it is
        // in `hedge_orders` and supersedes nothing. It reached the venue and was
        // acked; nothing after it has been.
        e.hedge_orders.insert(
            "h1".into(),
            HedgeOrder {
                maker_order_id: "m1".into(),
                chain_id: "h1".into(),
                market_id: "SYNTH-K-YES".into(),
                venue: Venue::Kalshi,
                side: BookSide::Ask,
                price: "0.40".into(),
                qty: 5,
                cum_filled: 0,
                supersedes: None,
            },
        );
        e.oid_venue.insert("h1".into(), "venue-h1".into());

        // h2 is decided (and, in the story, withheld by the executor)...
        e.apply_hedge_plans(
            vec![("h1".into(), HedgePlan::Retry { qty: 5, price: "0.40".into() }, None)],
            after(t, 6.0),
        );
        // ...and h3 one interval later.
        e.apply_hedge_plans(
            vec![("h1".into(), HedgePlan::Retry { qty: 5, price: "0.40".into() }, None)],
            after(t, 12.0),
        );
        assert_eq!(e.hedge_orders["h3"].supersedes.as_deref(), Some("h2"), "the chain link");

        assert_eq!(
            superseded(&e.hedge_orders, &e.oid_venue, "h3"),
            Some(crate::exec::Superseded { venue_order_id: "venue-h1".into(), credited: 0 }),
            "h2 never reached the venue, so h3 must verify h1 — the attempt still outstanding"
        );

        // The credited count travels with it: an attempt whose partial fill WAS
        // seen is accounted for, and the executor compares against this rather
        // than against zero.
        e.hedge_orders.get_mut("h1").expect("h1").cum_filled = 4;
        assert_eq!(superseded(&e.hedge_orders, &e.oid_venue, "h3").expect("h1").credited, 4);

        // Once h2 IS acked it becomes the nearer answer and the walk stops there.
        e.oid_venue.insert("h2".into(), "venue-h2".into());
        assert_eq!(
            superseded(&e.hedge_orders, &e.oid_venue, "h3").expect("h2").venue_order_id,
            "venue-h2"
        );
        // A chain with no acked attempt resolves to nothing rather than looping
        // — that is `recover_place`'s case, not this path's.
        assert_eq!(superseded(&e.hedge_orders, &HashMap::new(), "h3"), None);
        // ...and a first attempt supersedes nothing.
        assert_eq!(superseded(&e.hedge_orders, &e.oid_venue, "h1"), None);
    }

    /// The alarm is a side effect of the ACT half, and it fires once. `alarmed`
    /// is what stops the second one, and `hedges_naked` is the gauge it feeds.
    #[test]
    fn an_alarm_is_recorded_on_the_obligation_and_counted_once() {
        let mut e = test_engine(RunCfg { hedge_retry: Some(pol()), ..test_cfg() });
        let t = t0();
        e.pending_hedges.insert("h1".into(), pending(Some("h1"), t));

        e.apply_hedge_plans(vec![("h1".into(), HedgePlan::Wait, Some("naked".into()))], t);
        assert!(e.pending_hedges["h1"].alarmed);
        assert_eq!(e.n_naked, 1);
        // the decide half will not offer a second one for the same obligation
        let plans = hedge_tick_plans(
            &mut e.cx,
            &pol(),
            &e.pending_hedges,
            &books("0.30", "0.31"),
            &HashMap::new(),
            &HashMap::new(),
            after(t, 600.0),
        );
        assert_eq!(plans[0].2, None, "an obligation alarms exactly once");
        assert_eq!(plans[0].1, HedgePlan::Wait, "and waiting is still the policy");
    }

    // ------------------------------------------- the halted venue (2026-07-30) ---

    /// THE thing that makes parking acceptable at all.
    ///
    /// Backing off is only defensible if the leg stays as loud as it was. This
    /// obligation is parked for the full window AND past its alarm horizon: the
    /// plan must say `Parked` and the alarm must still be produced, in the same
    /// tick. If the alarm ever moved under the plan — if `Parked` were folded in
    /// with `Retire`, say — a halted venue would silently absorb a naked leg, and
    /// that is strictly worse than 335 log lines.
    #[test]
    fn a_parked_obligation_is_quiet_about_retrying_and_loud_about_being_naked() {
        let t = t0();
        let mut p = pending(Some("h1"), t);
        // Parked PAST the 60s alarm horizon, and read from inside the park but
        // after the horizon — the only window where the two can disagree.
        p.parked_until = Some(after(t, 120.0));
        p.paused_strikes = 1;
        let mut cx = Cx::default();
        let plans = hedge_tick_plans(
            &mut cx,
            &pol(),
            &HashMap::from([("h1".to_string(), p)]),
            // A book that WOULD be taken — the anchor exactly. So the only
            // reason not to place is the park.
            &books("0.40", "0.41"),
            &HashMap::new(),
            &HashMap::new(),
            after(t, 61.0),
        );
        assert_eq!(plans[0].1, HedgePlan::Parked, "the venue is halted: do not re-place");
        let alarm = plans[0].2.as_deref().expect("a parked leg is still a naked leg");
        assert!(alarm.contains("NAKED 5x P"), "and it still says so out loud: {alarm}");
    }

    /// ...and the park EXPIRES. A halt that ended must not leave the obligation
    /// parked for ever — the park is a backoff, not a kill switch.
    #[test]
    fn the_park_expires_and_the_retry_becomes_due_again() {
        let t = t0();
        let mut p = pending(Some("h1"), t);
        p.parked_until = Some(after(t, 15.0));
        let mut cx = Cx::default();
        let plans = hedge_tick_plans(
            &mut cx,
            &pol(),
            &HashMap::from([("h1".to_string(), p)]),
            &books("0.40", "0.41"),
            &HashMap::new(),
            &HashMap::new(),
            after(t, 15.1),
        );
        assert_eq!(plans[0].1, HedgePlan::Retry { qty: 5, price: "0.40".into() });
    }

    /// The backoff is a BACKOFF: it grows, and it stops growing.
    ///
    /// A fixed second interval would be the same defect one order of magnitude
    /// quieter, and an uncapped one would turn a long halt into an unbounded
    /// naked window after the reopen.
    #[test]
    fn the_park_doubles_and_then_stops_at_the_ceiling() {
        let secs = |s: u32| venue_reopen_park(s).as_secs();
        assert_eq!((secs(1), secs(2), secs(3)), (15, 30, 60), "doubling");
        assert_eq!((secs(4), secs(20), secs(u32::MAX)), (60, 60, 60), "and capped");
    }

    /// The venue's refusal parks the obligation it belongs to — and nothing else.
    ///
    /// `on_place_result` is keyed through `hedge_orders` to the CHAIN, so a
    /// refusal on one attempt parks the obligation that attempt was made for,
    /// even after the id has moved on. A refusal naming an order that is not a
    /// live hedge attempt (a maker quote, a take-take leg 1) must change nothing:
    /// this backoff is the hedge retry's, and it is not licensed to reach the
    /// quoter.
    #[test]
    fn a_halted_refusal_parks_its_own_obligation_and_no_other() {
        let mut e = test_engine(RunCfg { hedge_retry: Some(pol()), ..test_cfg() });
        let t = t0();
        e.pending_hedges.insert("h1".into(), pending(Some("h2"), t));
        e.pending_hedges.insert("h9".into(), pending(Some("h9"), t));
        // h2 is h1's SECOND attempt: same chain, different id.
        e.hedge_orders.insert(
            "h2".into(),
            HedgeOrder {
                maker_order_id: "m1".into(),
                chain_id: "h1".into(),
                market_id: "P".into(),
                venue: Venue::PolymarketUs,
                side: BookSide::Ask,
                price: "0.40".into(),
                qty: 5,
                cum_filled: 0,
                supersedes: Some("h1".into()),
            },
        );

        let halted = |oid: &str| {
            serde_json::json!({"kind":"place_result","venue":"polymarket_us",
                               "market_id":"P","order_id":oid,"ok":false,
                               "retry":"market_halted","error":"HTTP 409"})
        };
        e.on_place_result(&halted("h2"));
        assert!(e.pending_hedges["h1"].parked_until.is_some(), "the chain h2 belongs to");
        assert_eq!(e.pending_hedges["h1"].paused_strikes, 1);
        assert!(e.pending_hedges["h9"].parked_until.is_none(), "a bystander obligation");
        assert_eq!(e.n_parked, 1);

        // A refusal for an id that is no hedge of ours at all.
        e.on_place_result(&halted("q77"));
        assert_eq!(e.n_parked, 1, "a refused maker quote is not a hedge park");

        // ...and a refusal the classifier did NOT call halted keeps the ordinary
        // retry, which is the whole point of `Retry::Now` being the default.
        let mut ordinary = halted("h2");
        ordinary["retry"] = serde_json::json!("something_else");
        e.on_place_result(&ordinary);
        assert_eq!(e.pending_hedges["h1"].paused_strikes, 1, "only halted parks");
    }

    /// A halt that outlasts the first park backs off further, and the park is
    /// always measured from the LATEST refusal — not from the first.
    #[test]
    fn a_second_refusal_lengthens_the_park() {
        let mut e = test_engine(RunCfg { hedge_retry: Some(pol()), ..test_cfg() });
        let t = t0();
        e.pending_hedges.insert("h1".into(), pending(Some("h1"), t));
        e.hedge_orders.insert(
            "h1".into(),
            HedgeOrder {
                maker_order_id: "m1".into(),
                chain_id: "h1".into(),
                market_id: "P".into(),
                venue: Venue::PolymarketUs,
                side: BookSide::Ask,
                price: "0.40".into(),
                qty: 5,
                cum_filled: 0,
                supersedes: None,
            },
        );
        let halted = serde_json::json!({"kind":"place_result","venue":"polymarket_us",
                                        "market_id":"P","order_id":"h1","ok":false,
                                        "retry":"market_halted","error":"HTTP 409"});
        e.on_place_result(&halted);
        let first = e.pending_hedges["h1"].parked_until.expect("parked");
        e.on_place_result(&halted);
        let second = e.pending_hedges["h1"].parked_until.expect("still parked");
        assert_eq!(e.pending_hedges["h1"].paused_strikes, 2);
        assert!(
            second.saturating_duration_since(first) >= VENUE_REOPEN_PARK_FIRST,
            "strike 2 parks a full step longer than strike 1"
        );
    }
}
