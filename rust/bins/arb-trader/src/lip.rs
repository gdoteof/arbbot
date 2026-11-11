//! Kalshi liquidity-incentive (LIP) quoting. OFF unless `--lip <config>`.
//!
//! WHAT IT EARNS. Kalshi pays each market's incentive pool across one-second
//! snapshots of resting size near the touch. Per side, the reference price is
//! the first level where cumulative size reaches `TARGET / 5`. Levels at or
//! better than it score x1.0, and a level `n` ticks worse scores `DISCOUNT^n`.
//! A side short of `TARGET` scores nothing, and a snapshot pays only when BOTH
//! sides reach it. Every program listed on 2026-11-10 used a target of 1,000
//! and a discount of 0.5. A 200-lot at the best price is therefore its own
//! reference level, and everything resting behind it is discounted. On a thin
//! book that models a large share, for as long as the size behind it stays
//! where it is. The tape replay is in `data/scan/rewards-2026-11-10/`.
//!
//! WHAT IT DOES. For each configured Kalshi market it rests one YES bid and one
//! YES ask and holds whatever fills, with no hedge. A market is quoted only
//! while its book, scored by the rule above with our two orders in it, models
//! at least `min_usd_day` averaged over about ten minutes ([`MODEL_TAU_S`]). At
//! most `max_markets` are quoted at once, the best-modelled ones. The model is
//! the rule applied to the book we see; Kalshi's payout is the only check on it.
//!
//! WHERE AN ORDER RESTS ([`want`]). The rule pays an order at or better than
//! the reference price in full, and the reference price is the first level
//! where size adds up to [`REF_SIZE`], not the best price. Makers with
//! thousands of contracts follow that level: on 2026-11-06..10, when an order
//! under 500 contracts made a better price the reference level, 500 or more
//! joined that price within five seconds 43-59% of the time on the gas-min
//! books and 75% on the gas-max asks (`data/scan/rewards-2026-11-11/`). An
//! order that makes its own reference level therefore models a share it holds
//! for seconds, and ends up first in the queue ahead of the size that followed
//! it. So on each side:
//!   * An order that would ADD to the position joins the reference level the
//!     OTHER makers already make, behind them in the queue, and never makes
//!     one. What that models is what it keeps. It is pulled on the first pass
//!     that finds fewer than `REF_SIZE` contracts would fill before it
//!     ([`cover`]); an order placed in the last [`MIN_REQUOTE`] is not judged
//!     yet, because the book may not show it.
//!   * An order that would SHRINK the position rests at the touch, sized to
//!     the position so it cannot cross zero. The touch is the first level
//!     where other makers' size adds up to `min_lead` ([`lead`]). On
//!     2026-11-06..10 the best price on one side or the other of a gas-min
//!     book stood two or more cents inside the first 20 contracts 44% of the
//!     time (22-61% by market), and an order that joins the best price goes
//!     where those few contracts go. Where the queue ahead of it at the
//!     touch is [`BURIED`] times its size, it rests one cent inside instead.
//!
//! A market holding inventory that is not quoted still rests the order that
//! shrinks it ([`reducing`]). That applies whether the market fell below the
//! floor, lost its slot, or its program ended. The reward decides whether a
//! market is worth quoting; it does not decide whether held inventory gets a
//! way out.
//!
//! WHAT KEEPS IT APART FROM THE ARB ENGINE. Kalshi nets YES against NO in one
//! market, so this inventory and an arb basket's Kalshi leg cannot share a
//! market without corrupting each other. At boot the trader refuses any
//! configured market an open ledger lot holds. It then leaves alone every
//! market this module quotes or its log holds inventory on ([`left_alone`]).
//! That holds on every live run, `--lip` or not, because the config line that
//! opened a position may be gone. "Left alone" means four things:
//!   * no quoter, so no entries and no take-take;
//!   * no positions-recon finding, so nothing against the naked limit and
//!     nothing for recon-act to complete;
//!   * no boot adoption of a hedge's naked leg there;
//!   * a venue position the log never booked refuses the market here too.
//!
//! Fills arrive on the engine's account-wide fill feed and wake this loop
//! ([`owns_order`]). A frame's cumulative count is booked on arrival. The
//! per-order read, made on the frame and on the heartbeat, can only add to it.
//!
//! WHAT STOPS IT. A market rests nothing when any of these hold:
//!   * the kill file;
//!   * an untrusted or pulled feed, or a sequence gap (the engine drops the book);
//!   * an entry blackout on a relationship the market belongs to;
//!   * a place whose answer was lost, until the venue's order list accounts for it;
//!   * the loss stop;
//!   * a `quiet_utc` window.
//!
//! Past the market's `until`, or once its config line is gone, it is no
//! longer quoted, but inventory held there still rests its reducing side.
//!
//! The loss stop fires when inventory P&L marked to mid reaches
//! `-loss_stop_usd`. It writes `<log>.STOP` and pulls everything until a human
//! removes that file.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arb_core::model::Level;
use arb_venue::gateway::{CancelBy, CancelRequest, PlaceRequest, Side, Tif};
use arb_venue::VenueError;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::sink::OrderSink;

/// Kalshi's LIP target size and per-tick discount: the same on all 4,664
/// programs listed 2026-11-10.
const TARGET: f64 = 1000.0;
const DISCOUNT: f64 = 0.5;
/// One cent, in the ten-thousandths prices are carried in here. The discount
/// counts ticks of it.
const TICK: i64 = 100;
/// The soonest a side's order is replaced after the last place or cancel on
/// that side. A thin book that flickers would otherwise churn a 200-lot on
/// every delta, and each replace spends order-path budget.
const MIN_REQUOTE: Duration = Duration::from_secs(2);
/// The soonest a resting order is moved to a new price. Touch changes on
/// these books mostly flicker: on 2026-11-09 the busiest gas market's touch
/// moved 1,017 times for 2 s or longer but 312 times for 15 s or longer, and
/// every move followed is one more row of order history (`max_places_day`).
/// An order a flicker leaves one tick behind still scores, at half weight.
const REPRICE_HOLD: Duration = Duration::from_secs(15);
/// How often each resting order is checked at the venue with no fill frame to
/// prompt it. This is the backstop for an order the venue cancelled, or an
/// engine sweep pulled, without telling us.
const HEARTBEAT: Duration = Duration::from_secs(120);
/// How often the blackout file is re-read.
const BLACKOUT_REREAD: Duration = Duration::from_secs(30);
/// Markets already quoted stay ahead of a challenger unless it models this many
/// times better, and stay eligible down to `min_usd_day / STICKY`. Without the
/// margin, two markets of similar value would trade places on every book
/// change, and each swap cancels and re-places 200-lots on both.
const STICKY: f64 = 1.25;
/// Contracts that make a level the reference level.
const REF_SIZE: f64 = TARGET / 5.0;
/// A move toward the other side of the book is made only once the closer
/// price has been the wanted one for this long without a break: a level that
/// shows there and is gone seconds later leaves whoever followed it alone at
/// the new price. An order placed where nothing rests waits this long after
/// the last place or cancel on its side. Backing away waits [`REPRICE_HOLD`].
const STEP_IN_HOLD: Duration = Duration::from_secs(60);
/// A reducing order rests one tick inside the touch when the size ahead of it
/// there is this many times its own: behind a thousand contracts a 100-lot is
/// not reached.
const BURIED: f64 = 5.0;
/// One dollar, the price a contract pays out, in ten-thousandths.
const PAR: i64 = 10_000;
/// Kalshi's maker fee rate, on series that charge makers.
const MAKER_FEE: f64 = 0.0175;
/// The time constant, in seconds, of the mean that markets are picked on.
/// Kalshi pays on each side's share over time. The share on one book swings
/// with other makers' orders:
///   * on 2026-11-11, a 144-lot that came and went every ~15 s one tick behind
///     us moved gas-min 3.75 between $3.34 and $2.16 a day;
///   * 2,000-lots at the touch of gas-avg 3.70 moved it from $4.93 to $1.80
///     within a second.
///
/// Picked on the instant figure, every swing across `min_usd_day` or
/// `min_usd_day / STICKY` cancelled and re-placed both orders: 183 places in
/// one hour, against `max_places_day`. Ten minutes spans dozens of those
/// swings. A market whose value really falls is still dropped about as fast.
const MODEL_TAU_S: f64 = 600.0;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketCfg {
    /// Kalshi ticker.
    pub market: String,
    /// The program's pool in USD per day: `period_reward / 10_000` over the
    /// period's days.
    pub usd_day: f64,
    /// When the program ends, `YYYY-MM-DDTHH:MM:SSZ`. Nothing is quoted past
    /// it; inventory held there still rests the side that shrinks it.
    pub until: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cfg {
    /// Most contracts in one order. An order is also never more than keeps the
    /// position inside `cap`, nor more than the position it shrinks (see
    /// [`want`]), so with `cap` at or under `qty` it is `cap` that binds: at
    /// qty 200 / cap 100 a flat market rests 100 a side.
    pub qty: i64,
    /// Largest net position held either way, per market.
    pub cap: i64,
    /// Contracts other makers must show, added up from their best price, for
    /// a level to count as the touch ([`lead`]). A reducing order rests there,
    /// and the inventory is marked between the two touches.
    pub min_lead: i64,
    /// Daily windows, `["HH:MM", "HH:MM"]` in UTC, in which nothing rests,
    /// reducing orders included. A window may wrap midnight.
    pub quiet_utc: Vec<[String; 2]>,
    /// Most markets quoted at once.
    pub max_markets: usize,
    /// A market is quoted only while its book models at least this, in USD a
    /// day, with our orders in it.
    pub min_usd_day: f64,
    /// Inventory P&L, marked to mid across every market, at which everything
    /// is pulled and the stop file written.
    pub loss_stop_usd: f64,
    /// Places allowed per UTC day, across every market. Once spent, every quote
    /// is pulled until the day turns. Every place adds a row to the Kalshi
    /// order history, which the resting-order sweeps page through and fail
    /// closed past (`K_ORDERS_MAX_PAGES` in arb-venue). Unbounded re-quoting
    /// would grow it toward the point where the trader can neither start nor
    /// sweep clean.
    pub max_places_day: u64,
    pub markets: Vec<MarketCfg>,
}

/// Read and check the config. Every refusal names the field.
pub fn load(path: &str) -> Result<Cfg, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let cfg: Cfg = serde_yaml::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
    if cfg.qty <= 0 || cfg.cap <= 0 {
        return Err(format!("{path}: need qty > 0 and cap > 0, got qty {} cap {}", cfg.qty, cfg.cap));
    }
    if cfg.min_lead < 0 {
        return Err(format!("{path}: min_lead must be >= 0"));
    }
    if let Some(w) = cfg.quiet_utc.iter().find(|w| hhmm(&w[0]).is_none() || hhmm(&w[1]).is_none() || w[0] == w[1]) {
        return Err(format!("{path}: quiet_utc window {w:?} must be two different HH:MM times"));
    }
    if cfg.max_markets == 0 || cfg.markets.is_empty() || cfg.max_places_day == 0 {
        return Err(format!("{path}: nothing may be quoted (max_markets 0, max_places_day 0, or no markets)"));
    }
    let positive = |x: f64| x.is_finite() && x > 0.0;
    let non_negative = |x: f64| x.is_finite() && x >= 0.0;
    if !positive(cfg.loss_stop_usd) || !non_negative(cfg.min_usd_day) {
        return Err(format!("{path}: loss_stop_usd must be > 0 and min_usd_day >= 0"));
    }
    let mut seen = BTreeSet::new();
    for m in &cfg.markets {
        if !seen.insert(m.market.as_str()) {
            return Err(format!("{path}: {} is listed twice", m.market));
        }
        if crate::taketake::parse_iso8601_z(&m.until).is_none() {
            return Err(format!("{path}: {} `until` must be YYYY-MM-DDTHH:MM:SSZ", m.market));
        }
        if !positive(m.usd_day) {
            return Err(format!("{path}: {} usd_day must be > 0", m.market));
        }
    }
    Ok(cfg)
}

/// `HH:MM` as seconds into the day.
fn hhmm(s: &str) -> Option<u32> {
    let (h, m) = s.split_once(':')?;
    let ok = |x: &str| x.len() == 2 && x.bytes().all(|b| b.is_ascii_digit());
    let (h, m): (u32, u32) = (ok(h).then(|| h.parse().ok())??, ok(m).then(|| m.parse().ok())??);
    (h < 24 && m < 60).then_some(h * 3600 + m * 60)
}

/// Is wall time `now` inside one of the daily UTC windows?
pub(crate) fn quiet(windows: &[[String; 2]], now: f64) -> bool {
    let s = (now.max(0.0) as u64 % 86_400) as u32;
    windows.iter().any(|w| match (hhmm(&w[0]), hhmm(&w[1])) {
        (Some(a), Some(b)) if a <= b => a <= s && s < b,
        (Some(a), Some(b)) => s >= a || s < b,
        _ => false,
    })
}

// ---------------------------------------------------------------------------
// Prices and the scoring rule (pure)
// ---------------------------------------------------------------------------

/// A YES price in ten-thousandths of a dollar: "0.9600" -> 9600. Decimal
/// parsing, so no float rounds a price. Outside [0, 1] is `None`.
pub(crate) fn px(s: &str) -> Option<i64> {
    let (whole, frac) = s.trim().split_once('.').unwrap_or((s.trim(), ""));
    if frac.len() > 4 || !whole.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let w: i64 = if whole.is_empty() { 0 } else { whole.parse().ok()? };
    let f: i64 = if frac.is_empty() { 0 } else { format!("{frac:0<4}").parse().ok()? };
    let p = w.checked_mul(10_000)?.checked_add(f)?;
    (0..=10_000).contains(&p).then_some(p)
}

/// The wire spelling of a ten-thousandths price.
pub(crate) fn px_str(p: i64) -> String {
    format!("{}.{:04}", p / 10_000, p % 10_000)
}

/// One side of a book with our own resting order taken out: `(price, size)`,
/// best first, in the order the levels arrive in, dropping levels left empty.
/// `own` is our order on this side: `(price, contracts still resting)`.
pub(crate) fn others(levels: &[Level], own: Option<(i64, i64)>) -> Levels {
    levels
        .iter()
        .filter_map(|l| {
            let p = px(&l.price)?;
            let mut s: f64 = l.size.parse().ok()?;
            if let Some((op, oq)) = own {
                if op == p {
                    s -= oq as f64;
                }
            }
            (s > 1e-9).then_some((p, s))
        })
        .collect()
}

/// Our share of one side's score under the LIP rule, with `ours` resting in
/// it. `None` when the side is short of `TARGET`: it scores nothing, and a
/// snapshot needs both sides. `levels` are the OTHER makers' levels, best first
/// (bids descending, asks ascending), and `ours` is `(price, size)`.
///
/// The walk counts whole levels until the cumulative size reaches `TARGET`,
/// as the replay that priced this program did.
pub(crate) fn side_share(levels: &[(i64, f64)], ours: Option<(i64, f64)>, bids: bool) -> Option<f64> {
    let better = |a: i64, b: i64| if bids { a > b } else { a < b };
    // Merge ours in at its price: (price, size, is_ours), best first.
    let mut walk: Vec<(i64, f64, bool)> = Vec::with_capacity(levels.len() + 1);
    let mut placed = ours.is_none();
    for &(p, s) in levels {
        if let (false, Some((op, os))) = (placed, ours) {
            if better(op, p) || op == p {
                walk.push((op, os, true));
                placed = true;
            }
        }
        walk.push((p, s, false));
    }
    if let (false, Some((op, os))) = (placed, ours) {
        walk.push((op, os, true));
    }
    let mut cum = 0.0;
    let mut reference: Option<i64> = None;
    let mut counted = 0usize;
    let mut i = 0usize;
    while i < walk.len() {
        let p = walk[i].0;
        while i < walk.len() && walk[i].0 == p {
            cum += walk[i].1;
            i += 1;
        }
        counted = i;
        if reference.is_none() && cum >= REF_SIZE {
            reference = Some(p);
        }
        if cum >= TARGET {
            break;
        }
    }
    if cum < TARGET {
        return None;
    }
    let reference = reference?;
    let (mut total, mut mine) = (0.0, 0.0);
    for &(p, s, me) in &walk[..counted] {
        let worse_ticks = if bids { reference - p } else { p - reference };
        let w = if worse_ticks <= 0 { 1.0 } else { DISCOUNT.powi((worse_ticks / TICK) as i32) };
        total += s * w;
        if me {
            mine += s * w;
        }
    }
    Some(if total > 0.0 { mine / total } else { 0.0 })
}

/// The pool share our two orders would score on this book: the mean of the
/// two sides' shares, or 0 if either side is short of `TARGET`.
pub(crate) fn modelled_share(
    bids_o: &[(i64, f64)],
    asks_o: &[(i64, f64)],
    bid: Option<(i64, i64)>,
    ask: Option<(i64, i64)>,
) -> f64 {
    let f = |o: Option<(i64, i64)>| o.map(|(p, q)| (p, q as f64));
    match (side_share(bids_o, f(bid), true), side_share(asks_o, f(ask), false)) {
        (Some(a), Some(b)) => (a + b) / 2.0,
        _ => 0.0,
    }
}

/// What [`want`] and [`stay`] size and place orders by: `qty`, `cap` and
/// `min_lead` from the config. A market that only reduces has a `cap` of zero.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Sizing {
    pub qty: i64,
    pub cap: i64,
    pub min_lead: i64,
}

/// The first price, from the other makers' best, where their size adds up to
/// `size`. `None` when the whole side is smaller.
pub(crate) fn lead(levels: &[(i64, f64)], size: f64) -> Option<i64> {
    let mut cum = 0.0;
    levels.iter().find_map(|&(p, s)| {
        cum += s;
        (cum >= size).then_some(p)
    })
}

/// One side's order: `(price, contracts)`. `own` are the other makers' levels
/// on this side, best first, and `opp` is their best price on the other side.
fn want_side(own: &[(i64, f64)], opp: Option<i64>, bids: bool, pos: i64, s: &Sizing) -> Option<(i64, i64)> {
    // Contracts held on the side a fill here adds to; negative when a fill
    // here shrinks the position.
    let held = if bids { pos } else { -pos };
    if held >= 0 {
        let n = (s.cap - held).min(s.qty);
        return lead(own, REF_SIZE).filter(|_| n > 0).map(|p| (p, n));
    }
    let touch = lead(own, s.min_lead as f64)?;
    let n = (-held).min(s.qty);
    let (mut ahead, mut queue) = (0.0, 0.0);
    for &(p, z) in own {
        if p == touch {
            queue = z;
            break;
        }
        ahead += z;
    }
    let inside = if bids { touch + TICK } else { touch - TICK };
    // One tick inside must be a price, and leave a tick between it and the
    // other side.
    let fits = (TICK..=PAR - TICK).contains(&inside)
        && opp.is_none_or(|o| if bids { inside + TICK < o } else { inside - TICK > o });
    // Inside the touch it stays too small to become the reference level: the
    // size it stepped in front of would follow it there, and it would be
    // buried again a tick closer to the other side.
    let small = n.min((REF_SIZE - ahead).ceil() as i64 - 1);
    Some(if queue >= BURIED * n as f64 && fits && small > 0 { (inside, small) } else { (touch, n) })
}

/// The orders this market should rest: `(price, contracts)` per side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Want {
    pub bid: Option<(i64, i64)>,
    pub ask: Option<(i64, i64)>,
}

/// Where each side's order belongs (see the module docs). A side where the
/// other makers show no reference level gets no adding order, and one where
/// they show less than `min_lead` gets no reducing order. A book whose others'
/// best bid is at or through their best ask gets nothing: post-only would
/// refuse it, and a crossed book is one we cannot see properly.
pub(crate) fn want(bids_o: &[(i64, f64)], asks_o: &[(i64, f64)], pos: i64, s: &Sizing) -> Want {
    let (b, a) = (bids_o.first().map(|l| l.0), asks_o.first().map(|l| l.0));
    if let (Some(b), Some(a)) = (b, a) {
        if b >= a {
            return Want::default();
        }
    }
    Want { bid: want_side(bids_o, a, true, pos, s), ask: want_side(asks_o, b, false, pos, s) }
}

/// Contracts that would fill before an order resting at `price`: every one at
/// a better price, and `ahead` of those at its own.
pub(crate) fn cover(price: i64, ahead: f64, own: &[(i64, f64)], bids: bool) -> f64 {
    own.iter()
        .map(|&(p, z)| match p.cmp(&price) {
            std::cmp::Ordering::Equal => z.min(ahead),
            o if (o == std::cmp::Ordering::Greater) == bids => z,
            _ => 0.0,
        })
        .sum()
}

/// What to do with a resting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stay {
    Keep,
    /// Replace it at the wanted price, once the hold for that move has
    /// passed. `closer` when the move is toward the other side of the book.
    Reprice { closer: bool },
    /// Cancel it now.
    Pull,
}

/// Should a resting order stay? `resting` and `want` are `(price, contracts)`,
/// `ahead` is the order's [`Order::ahead`], and `own` are the other makers'
/// levels on its side, best first.
///
/// It is pulled at once when nothing is wanted there or a full fill could take
/// the position past the cap or through zero. An adding order is also pulled
/// at once when fewer than `REF_SIZE` contracts would fill before it: the size
/// it stood behind has traded or left, and what is left of the level is this
/// order. Otherwise it stays while it is at the wanted price with at least half
/// the wanted size (under half, its score is mostly gone). A reducing order
/// also stays one tick inside the touch, so a queue that comes and goes at the
/// touch does not move it back and forth. And it stays at the touch while
/// fewer than [`BURIED`] times its size are ahead of it there: what makes the
/// level deep joined behind it, and stepping inside would give up a cent to
/// get ahead of size it is already ahead of.
pub(crate) fn stay(resting: (i64, i64), ahead: f64, want: Option<(i64, i64)>, own: &[(i64, f64)], bids: bool, pos: i64, s: &Sizing) -> Stay {
    let ((rp, rq), Some((wp, wq))) = (resting, want) else { return Stay::Pull };
    let held = if bids { pos } else { -pos };
    let pull = match held < 0 {
        true => rq > -held,
        false => rq > s.cap - held || cover(rp, ahead, own, bids) < REF_SIZE,
    };
    let touch = lead(own, s.min_lead as f64);
    let level = own.iter().find(|l| l.0 == rp).map_or(0.0, |l| l.1);
    let settled = held < 0
        && (touch == Some(if bids { rp - TICK } else { rp + TICK })
            || touch == Some(rp) && ahead.min(level) < BURIED * rq as f64);
    if pull {
        Stay::Pull
    } else if rq * 2 >= wq && (rp == wp || settled) {
        Stay::Keep
    } else {
        Stay::Reprice { closer: if bids { wp > rp } else { wp < rp } }
    }
}

/// The mark: midway between the two touches. A side whose makers show less
/// than `min_lead` in all is read at its best price, so a book that thins out
/// on one side still moves the mark. `None` when a side is empty.
pub(crate) fn mark_mid(bids_o: &[(i64, f64)], asks_o: &[(i64, f64)], min_lead: i64) -> Option<i64> {
    let at = |side: &[(i64, f64)]| lead(side, min_lead as f64).or(side.first().map(|l| l.0));
    Some((at(bids_o)? + at(asks_o)?) / 2)
}

/// One market as [`select`] sees it.
#[derive(Debug, Clone)]
pub(crate) struct Cand {
    pub market: String,
    /// What its book models for our two orders, USD a day.
    pub usd_day: f64,
    /// Quoted on the last pass.
    pub quoted: bool,
    /// Holds inventory.
    pub held: bool,
    /// Nothing blocks it: no kill, stop, blackout, program end, or missing book.
    pub open: bool,
    /// Nothing blocks an order that shrinks the position: `open`, or blocked
    /// only because the program is over or the config line is gone.
    pub exits: bool,
}

/// Which markets to quote, at most `max`.
///
/// A mean over time, each value weighted by how long it held, decaying with
/// [`MODEL_TAU_S`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Mean {
    last: f64,
    mean: f64,
    at: Option<f64>,
}

impl Mean {
    /// `x` holds from wall time `t` on. The previous value held until `t`.
    pub(crate) fn push(&mut self, x: f64, t: f64) {
        self.mean = match self.at {
            None => x,
            Some(t0) => {
                let a = 1.0 - (-(t - t0).max(0.0) / MODEL_TAU_S).exp();
                self.mean + a * (self.last - self.mean)
            }
        };
        self.last = x;
        self.at = Some(t);
    }

    pub(crate) fn get(&self) -> f64 {
        self.mean
    }
}

/// Every market holding inventory takes a slot first, whether or not it can be
/// quoted, so inventory is never spread over more than `max` markets: about
/// `max * cap` contracts at most. Inventory on several strikes of one figure
/// is one bet, and nothing else here bounds it across markets. A held market is
/// quoted while it models at least `min / STICKY`. The slots left go to the
/// best other markets modelling at least `min`, where one already quoted ranks
/// as if it modelled `STICKY` times its value and stays down to `min / STICKY`.
pub(crate) fn select(cands: &[Cand], min: f64, max: usize) -> BTreeSet<String> {
    let passes = |c: &Cand| {
        let floor = if c.quoted || c.held { min / STICKY } else { min };
        c.open && c.usd_day > 0.0 && c.usd_day >= floor
    };
    let rank = |c: &Cand| if c.quoted { c.usd_day * STICKY } else { c.usd_day };
    let by_rank = |a: &&Cand, b: &&Cand| {
        rank(b).partial_cmp(&rank(a)).unwrap_or(std::cmp::Ordering::Equal).then(a.market.cmp(&b.market))
    };
    let mut held: Vec<&Cand> = cands.iter().filter(|c| c.held).collect();
    held.sort_by(by_rank);
    let mut out = BTreeSet::new();
    let mut slots = max;
    for c in held {
        if slots == 0 {
            break;
        }
        slots -= 1;
        if passes(c) {
            out.insert(c.market.clone());
        }
    }
    let mut rest: Vec<&Cand> = cands.iter().filter(|c| !c.held && passes(c)).collect();
    rest.sort_by(by_rank);
    out.extend(rest.into_iter().take(slots).map(|c| c.market.clone()));
    out
}

/// The held markets [`select`] left out that may still rest an order. Each
/// rests only the side that shrinks its position, sized to the position:
/// [`want`] with a cap of zero. Such an order never adds inventory, so it
/// needs no slot.
pub(crate) fn reducing(cands: &[Cand], quoted: &BTreeSet<String>) -> BTreeSet<String> {
    cands.iter().filter(|c| c.held && c.exits && !quoted.contains(&c.market)).map(|c| c.market.clone()).collect()
}

// ---------------------------------------------------------------------------
// Accounting (pure)
// ---------------------------------------------------------------------------

/// One market's inventory since the quoter first ran on it: net YES contracts,
/// signed cash in ten-thousandths, and maker fees in dollars.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Book {
    pub pos: i64,
    pub cash_e4: i64,
    pub fees_usd: f64,
}

impl Book {
    /// `n` contracts filled on our `bid` (bought YES) or ask (sold YES) at `p`.
    /// The YES axis throughout: a sold YES we did not hold is a short whose
    /// settlement pays out `pos * outcome`. That marks the same as Kalshi's
    /// NO purchase at `1 - p`.
    pub fn fill(&mut self, bid: bool, p: i64, n: i64, charges_makers: bool) {
        if bid {
            self.pos += n;
            self.cash_e4 -= p * n;
        } else {
            self.pos -= n;
            self.cash_e4 += p * n;
        }
        if charges_makers {
            let pf = p as f64 / 10_000.0;
            self.fees_usd += MAKER_FEE * pf * (1.0 - pf) * n as f64;
        }
    }

    /// Dollars if the position were closed at `mid` (ten-thousandths).
    pub fn pnl_at(&self, mid: i64) -> f64 {
        (self.cash_e4 + self.pos * mid) as f64 / 10_000.0 - self.fees_usd
    }
}

/// Fold the log's `fill` and `adopt` records into per-market books.
pub(crate) fn replay(records: &[Value]) -> BTreeMap<String, Book> {
    let mut out: BTreeMap<String, Book> = BTreeMap::new();
    for r in records {
        let ev = r.get("event").and_then(Value::as_str);
        let (Some(m), Some(n), Some(p)) = (
            r.get("market").and_then(Value::as_str),
            r.get("qty").and_then(Value::as_i64),
            r.get("price").and_then(Value::as_str).and_then(px),
        ) else {
            continue;
        };
        let b = out.entry(m.to_string()).or_default();
        match ev {
            Some("fill") => {
                let bid = r.get("side").and_then(Value::as_str) == Some("bid");
                b.fill(bid, p, n, arb_core::fees::kalshi_charges_makers(m));
            }
            // An adoption carries a SIGNED qty and books no fee: it is the
            // venue's word for contracts the log missed, entered at the mid.
            Some("adopt") => {
                b.pos += n;
                b.cash_e4 -= p * n;
            }
            _ => {}
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The engine's side of the seam
// ---------------------------------------------------------------------------

/// One market's book: `(bids, asks)`, best first.
type Sides = (Vec<Level>, Vec<Level>);
/// One side of a book as `(price, size)`, best first.
type Levels = Vec<(i64, f64)>;
/// An entry-blackout window as `maker_exit::read_entry_blackouts` returns it.
type Window = (String, f64, f64, String);

/// A watched market's trusted book, and how far the size at each price has
/// fallen, added up over every change since the loop last took it (`[bids,
/// asks]`). The loop looks at the book once a pass. A level that is cancelled
/// and put back between two passes shows it the same size twice, and the fall
/// in between is only here.
#[derive(Clone, Default)]
struct Seen {
    sides: Sides,
    drops: [BTreeMap<i64, f64>; 2],
}

static BOOKS: Mutex<BTreeMap<String, Seen>> = Mutex::new(BTreeMap::new());
/// The markets this run's quoter tracks: every configured one, and every one
/// its log still holds inventory on. Set once before the feed starts, and only
/// by a `--lip` run.
static WATCHED: OnceLock<BTreeSet<String>> = OnceLock::new();
/// Kalshi markets the arb engine must leave alone: every market `--lip` quotes
/// or its log holds inventory on. Set on every live run, `--lip` or not, so the
/// inventory outlives the config line that opened it.
static LEFT: OnceLock<BTreeSet<String>> = OnceLock::new();
/// Venue ids of the orders this module placed, oldest first, for the fill
/// feed's attribution ([`owns_order`]). Kept here rather than in the engine's
/// sidecar list, which holds 256 ids: this module's re-quoting would evict the
/// maker exit's ids from it.
static OWN_IDS: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());
/// Venue ids of the orders this module is tracking right now: resting,
/// recovered, or owed a final count. Never evicted, however long they rest.
static LIVE_IDS: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
/// About a day of re-quoting at a busy pace. A fill on an order older than
/// that is reported as unattributed, which is the safe way to fail.
const OWN_IDS_REMEMBERED: usize = 4096;
/// Market -> (venue order id -> the highest cumulative count a fill frame
/// reported for it) since the last pass. Empty: the book moved.
static DIRTY: Mutex<BTreeMap<String, BTreeMap<String, i64>>> = Mutex::new(BTreeMap::new());
/// The latest stats object, for the engine's 60 s line.
static STATS: Mutex<Option<Value>> = Mutex::new(None);

fn notify() -> &'static tokio::sync::Notify {
    static N: OnceLock<tokio::sync::Notify> = OnceLock::new();
    N.get_or_init(tokio::sync::Notify::new)
}

fn mark(market: &str, filled: Option<(&str, i64)>) {
    if let Ok(mut g) = DIRTY.lock() {
        let ids = g.entry(market.to_owned()).or_default();
        if let Some((id, cum)) = filled {
            let c = ids.entry(id.to_owned()).or_insert(cum);
            *c = (*c).max(cum);
        }
    }
    notify().notify_one();
}

/// Set the markets this run's quoter tracks (empty without `--lip`) and the
/// markets the arb engine must leave alone. Called once, before the feed starts.
pub fn install(watched: BTreeSet<String>, left: BTreeSet<String>) {
    let _ = WATCHED.set(watched);
    let _ = LEFT.set(left);
}

/// The markets this module tracks; empty on every run without `--lip`.
pub fn watched() -> Vec<String> {
    WATCHED.get().map(|w| w.iter().cloned().collect()).unwrap_or_default()
}

/// Is `market` one this module tracks? False on every run without `--lip`.
pub fn watching(market: &str) -> bool {
    WATCHED.get().is_some_and(|w| w.contains(market))
}

/// Must the arb engine leave this Kalshi market alone? True for every market
/// `--lip` quotes or its log holds inventory on: Kalshi would net an arb leg
/// there against that inventory. False on every run whose log holds nothing,
/// without `--lip`.
pub fn left_alone(kalshi_market: &str) -> bool {
    LEFT.get().is_some_and(|l| l.contains(kalshi_market))
}

/// Did this module place the venue order `venue_order_id`?
pub fn owns_order(venue_order_id: &str) -> bool {
    LIVE_IDS.lock().is_ok_and(|g| g.contains(venue_order_id))
        || OWN_IDS.lock().is_ok_and(|g| g.iter().any(|id| id == venue_order_id))
}

fn note_order(venue_order_id: &str) {
    if let Ok(mut g) = OWN_IDS.lock() {
        if g.len() >= OWN_IDS_REMEMBERED {
            g.pop_front();
        }
        g.push_back(venue_order_id.to_owned());
    }
    if let Ok(mut g) = LIVE_IDS.lock() {
        g.insert(venue_order_id.to_owned());
    }
}

/// The order is done with: its final count is booked.
fn release_order(venue_order_id: &str) {
    if let Ok(mut g) = LIVE_IDS.lock() {
        g.remove(venue_order_id);
    }
}

/// What the log at `path` says this module holds.
#[derive(Debug, Default)]
pub struct Holdings {
    /// Nonzero net positions, by market, from every line that parses.
    pub held: BTreeMap<String, i64>,
    /// Every market any parsed line names.
    pub named: BTreeSet<String>,
    /// Why the log cannot be trusted whole, if it cannot: a line that does not
    /// parse (a crash mid-append leaves one), or a read failure.
    pub error: Option<String>,
}

/// Read the log at `path` line by line, keeping what parses. A missing log
/// holds nothing.
pub fn log_holdings(path: &str) -> Holdings {
    let text = match std::fs::read(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Holdings::default(),
        Err(e) => return Holdings { error: Some(e.to_string()), ..Holdings::default() },
    };
    let mut records = Vec::new();
    let mut bad = 0usize;
    for line in String::from_utf8_lossy(&text).lines() {
        let line = line.trim_matches(|c: char| c == '\0' || c.is_whitespace());
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(v) => records.push(v),
            Err(_) => bad += 1,
        }
    }
    let named = records.iter().filter_map(|r| r.get("market").and_then(Value::as_str)).map(String::from).collect();
    let held = replay(&records).into_iter().filter(|(_, b)| b.pos != 0).map(|(m, b)| (m, b.pos)).collect();
    let error = (bad > 0).then(|| format!("{bad} line(s) do not parse"));
    Holdings { held, named, error }
}

/// The engine saw `market`'s book move. `None` means it can no longer be
/// priced from: crossed, stamped in the future, or lost to a sequence gap.
pub fn book_changed(market: &str, book: Option<(&[Level], &[Level])>) {
    if let Ok(mut g) = BOOKS.lock() {
        match book {
            Some((b, a)) => {
                let seen = g.entry(market.to_owned()).or_default();
                for (i, (new, old)) in [(b, &seen.sides.0), (a, &seen.sides.1)].into_iter().enumerate() {
                    let now: BTreeMap<i64, f64> = others(new, None).into_iter().collect();
                    for (p, was) in others(old, None) {
                        let fell = was - now.get(&p).copied().unwrap_or(0.0);
                        if fell > 1e-9 {
                            *seen.drops[i].entry(p).or_default() += fell;
                        }
                    }
                }
                seen.sides = (b.to_vec(), a.to_vec());
            }
            None => {
                g.remove(market);
            }
        }
    }
    mark(market, None);
}

/// The feed can no longer be trusted, or the engine was killed. Every resting
/// order of ours is pulled on the next pass.
pub fn clear_books() {
    let Some(w) = WATCHED.get() else { return };
    if let Ok(mut g) = BOOKS.lock() {
        g.clear();
    }
    for m in w {
        mark(m, None);
    }
}

/// A fill frame named an order this module placed: `cum` contracts of it have
/// filled in all.
pub fn wake_fill(market: &str, venue_order_id: &str, cum: i64) {
    if watching(market) {
        mark(market, Some((venue_order_id, cum)));
    }
}

/// This module's gauges for the engine's stats line, once the loop has run.
pub fn stats() -> Option<Value> {
    STATS.lock().ok().and_then(|g| g.clone())
}

async fn dirty() -> BTreeMap<String, BTreeMap<String, i64>> {
    loop {
        if let Ok(mut g) = DIRTY.lock() {
            if !g.is_empty() {
                return std::mem::take(&mut *g);
            }
        }
        notify().notified().await;
    }
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

/// Everything `run` needs besides the config.
pub struct Wiring {
    pub kalshi: Arc<dyn OrderSink>,
    /// Append-only record of fills, adoptions and stops (`data/exec/lip.jsonl`).
    pub log_path: String,
    pub blackouts_path: String,
    pub kill_file: String,
    /// Relationship ids per configured market, for the blackout check.
    pub rels: BTreeMap<String, Vec<String>>,
    /// Decide and print; never place, cancel, or write the log or stop file.
    pub shadow: bool,
}

#[derive(Debug, Clone)]
struct Order {
    venue_id: String,
    price: i64,
    qty: i64,
    filled: i64,
    checked: Instant,
    /// Contracts at this order's own price that fill before it does: the
    /// other makers' size there when it joined, less every fall in the size at
    /// that price since ([`Seen::drops`]). The book does not say whether a
    /// fall was ahead of this order or behind it, so each one is taken to be
    /// ahead. That counts an order of ours cancelled at the same price just
    /// before this one was placed, which errs toward a pull.
    ahead: f64,
}

impl Order {
    fn resting(&self) -> i64 {
        self.qty - self.filled
    }

    /// Bring `ahead` up to date with the falls on this order's side since the
    /// last pass, by price.
    fn track(&mut self, drops: &BTreeMap<i64, f64>) {
        self.ahead = (self.ahead - drops.get(&self.price).copied().unwrap_or(0.0)).max(0.0);
    }
}

/// A place whose answer never came back. The venue may hold it under an id
/// this process has not learned.
struct Lost {
    bid: bool,
    req: PlaceRequest,
    next: Instant,
    tries: u32,
}

struct Mkt {
    cfg: MarketCfg,
    until: f64,
    bid: Option<Order>,
    ask: Option<Order>,
    /// The last place, cancel or whole fill per side (`[bid, ask]`), and the
    /// refusals since that side's last success.
    last_touch: [Option<Instant>; 2],
    fails: [u32; 2],
    /// Per side, a wanted price closer to the other side of the book than the
    /// resting order, and since when it has been the wanted one without a
    /// break ([`STEP_IN_HOLD`]).
    closer: [Option<(i64, Instant)>; 2],
    book: Book,
    /// The venue position not yet folded into `book`, awaiting a mid to adopt it at.
    unadopted: i64,
    mid: Option<i64>,
    /// What this market models in $/day, averaged over time ([`Mean`]). It
    /// picks the markets; the instant figure only feeds it.
    modelled: f64,
    model: Mean,
    /// Cancelled orders whose final fill count has not been read yet, by side
    /// (`true` = bid). Nothing new rests on the market until they are settled:
    /// a place sized off a position missing their fills could pass the cap.
    unsettled: Vec<(bool, Order)>,
    settle_next: Option<Instant>,
    settle_tries: u32,
    /// Places whose answer was lost. Nothing new rests on the market while any is.
    lost: Vec<Lost>,
    /// Why this market rests nothing for the rest of the process, if it doesn't.
    refused: Option<String>,
}

impl Mkt {
    /// Does the venue hold inventory here? The booked position plus what
    /// awaits adoption is the venue's count at boot. A market that settled
    /// while the log still shows a position reads as held only until the next
    /// boot, which sees the venue's zero. Its settlement is not booked here.
    fn held(&self) -> bool {
        self.book.pos + self.unadopted != 0
    }
}

/// The longest a side waits between attempts while the venue keeps refusing it.
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// How often a cancelled order's final count is re-read while it is owed one.
const SETTLE_RETRY: Duration = Duration::from_secs(5);
/// How often, and how many times, the venue's order list is searched for a
/// place whose answer was lost before the market is given up on. Ten minutes:
/// far past the list's lag behind a write.
const LOST_RETRY: Duration = Duration::from_secs(30);
const LOST_TRIES: u32 = 20;
/// Past `LOST_TRIES`, or `SETTLE_TRIES` for a final count: the stop file is
/// written and the read goes on at this pace for the life of the process.
const SLOW_RETRY: Duration = Duration::from_secs(300);
/// Five minutes of `SETTLE_RETRY`.
const SETTLE_TRIES: u32 = 60;

/// The wait before a side's next place after `fails` refusals in a row:
/// [`MIN_REQUOTE`], doubling per refusal, at most [`MAX_BACKOFF`]. A halted
/// market answers every place with the same refusal, and retrying it every
/// pass would spend the order-path budget the arb engine needs.
fn backoff(fails: u32) -> Duration {
    (MIN_REQUOTE * 2u32.pow(fails.min(5))).min(MAX_BACKOFF)
}

/// Did a place that came back with `e` certainly never rest? Only a venue
/// answer or a local refusal says so. A transport failure or an unreadable
/// answer may hide an accepted order.
///
/// A 5xx is not such an answer: a gateway in front of the venue can time out
/// on a place the venue went on to accept.
fn never_rested(e: &VenueError) -> bool {
    match e {
        VenueError::Status { status, .. } => *status < 500,
        VenueError::RateLimited { .. } | VenueError::Sign(_) | VenueError::NotWired => true,
        _ => false,
    }
}

/// Places made today (UTC), and which day that is.
static PLACES_DAY: Mutex<(u64, u64)> = Mutex::new((0, 0));

fn utc_day(now: f64) -> u64 {
    (now / 86_400.0) as u64
}

fn places_today(now: f64) -> u64 {
    PLACES_DAY.lock().map(|g| if g.0 == utc_day(now) { g.1 } else { 0 }).unwrap_or(u64::MAX)
}

fn count_place(now: f64) {
    if let Ok(mut g) = PLACES_DAY.lock() {
        let d = utc_day(now);
        *g = if g.0 == d { (d, g.1 + 1) } else { (d, 1) };
    }
}

/// A stop this process could not write down, or a shadow run's stop: held for
/// the life of the process.
static STOP_LATCHED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Stop everything: write the stop file, which pulls every quote until a human
/// removes it.
fn write_stop(w: &Wiring, why: &str) {
    if w.shadow {
        STOP_LATCHED.store(true, Ordering::Relaxed);
        eprintln!("[lip] SHADOW STOP: {why}");
        return;
    }
    let path = stop_file(&w.log_path);
    if let Err(e) = std::fs::write(&path, format!("{why}\nremove this file to resume\n")) {
        STOP_LATCHED.store(true, Ordering::Relaxed);
        ERRORS.fetch_add(1, Ordering::Relaxed);
        eprintln!("[lip] could not write {path} ({e}); stopped until this process exits");
    }
    append(w, json!({"ts": now_wall(), "event": "stop", "why": why}));
    eprintln!("[lip] ### STOP: {why}. Pulling every quote; {path} holds it until removed. What is held stays held.");
}

fn stopped(w: &Wiring) -> bool {
    STOP_LATCHED.load(Ordering::Relaxed) || (!w.shadow && exists(&stop_file(&w.log_path)))
}

static PLACED: AtomicU64 = AtomicU64::new(0);
static CANCELLED: AtomicU64 = AtomicU64::new(0);
static FILLED_CT: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);

fn now_wall() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// A fresh client id: `l` + milliseconds, strictly increasing.
fn next_coid() -> String {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let ms = (now_wall() * 1000.0) as u64;
    let mut prev = LAST.load(Ordering::Relaxed);
    loop {
        let next = ms.max(prev + 1);
        match LAST.compare_exchange(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return format!("l{next}"),
            Err(p) => prev = p,
        }
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, String> {
    tokio::task::spawn_blocking(f).await.map_err(|e| format!("task failed: {e}"))
}

fn append(w: &Wiring, rec: Value) {
    if w.shadow {
        eprintln!("[lip] SHADOW, not logged: {rec}");
        return;
    }
    if let Err(e) = crate::ledger::append(&w.log_path, &rec) {
        ERRORS.fetch_add(1, Ordering::Relaxed);
        eprintln!("[lip] could not append to {}: {e} — record was {rec}", w.log_path);
    }
}

fn stop_file(log_path: &str) -> String {
    format!("{log_path}.STOP")
}

fn exists(path: &str) -> bool {
    std::path::Path::new(path).exists()
}

pub async fn run(cfg: Cfg, w: Wiring) {
    let records = if !exists(&w.log_path) {
        Vec::new()
    } else {
        match crate::ledger::read(&w.log_path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[lip] REFUSED: {} is unreadable ({e}); the loss stop would be blind", w.log_path);
                return;
            }
        }
    };
    let books = replay(&records);
    let history: BTreeSet<&str> = records.iter().filter_map(|r| r.get("market").and_then(Value::as_str)).collect();
    // Venue truth for where each market stands.
    let venue = loop {
        let k = w.kalshi.clone();
        match blocking(move || k.net_positions().map_err(|e| e.to_string())).await {
            Ok(Ok(p)) => break p,
            Ok(Err(e)) | Err(e) => eprintln!("[lip] positions read failed ({e}); retrying in 30s"),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    };
    // The configured markets, then every market the log still holds inventory
    // on without a config line: tracked, marked and counted by the loss stop,
    // never quoted, resting only the side that shrinks the position.
    let mut lines: Vec<(MarketCfg, f64)> = cfg
        .markets
        .iter()
        .map(|m| (m.clone(), crate::taketake::parse_iso8601_z(&m.until).unwrap_or(0.0)))
        .collect();
    for (name, b) in &books {
        if b.pos != 0 && !cfg.markets.iter().any(|m| &m.market == name) {
            lines.push((MarketCfg { market: name.clone(), usd_day: 0.0, until: String::new() }, 0.0));
        }
    }
    let mut mkts: BTreeMap<String, Mkt> = BTreeMap::new();
    for (mc, until) in lines {
        let book = books.get(&mc.market).copied().unwrap_or_default();
        let held = venue.get(&mc.market).copied().unwrap_or(0.0).round() as i64;
        let (mut unadopted, mut refused) = (0, None);
        if held != book.pos {
            if history.contains(mc.market.as_str()) {
                // Fills the log missed (a crash between the fill and its
                // record): adopted at the first mid, so P&L counts from what
                // is actually held.
                unadopted = held - book.pos;
                eprintln!(
                    "[lip] {}: the venue holds {held} where {} accounts for {} — the difference \
                     is adopted at the first mid",
                    mc.market, w.log_path, book.pos
                );
            } else {
                let why = format!(
                    "the venue holds {held} contract(s) here that {} never booked, so they are \
                     not this module's to trade against",
                    w.log_path
                );
                eprintln!("[lip] {} REFUSED: {why}. Nothing rests here this run; CHECK BY HAND whose they are.", mc.market);
                refused = Some(why);
            }
        }
        mkts.insert(
            mc.market.clone(),
            Mkt {
                cfg: mc,
                until,
                bid: None,
                ask: None,
                last_touch: [None, None],
                fails: [0, 0],
                closer: [None, None],
                book,
                unadopted,
                mid: None,
                modelled: 0.0,
                model: Mean::default(),
                unsettled: Vec::new(),
                settle_next: None,
                settle_tries: 0,
                lost: Vec::new(),
                refused,
            },
        );
    }
    eprintln!(
        "[lip] {}: {} market(s) ({} configured), orders of up to {} joining the reference level \
         others make, cap {} net, exits at the first {} contracts others show, {} quiet window(s), \
         quoting the best {} modelling >= ${:.2}/day (every market holding inventory takes a \
         slot, and one not quoted rests only its reducing side), loss stop ${:.0}. Log {}.",
        if w.shadow { "SHADOW" } else { "ARMED" },
        mkts.len(),
        cfg.markets.len(),
        cfg.qty.min(cfg.cap),
        cfg.cap,
        cfg.min_lead,
        cfg.quiet_utc.len(),
        cfg.max_markets,
        cfg.min_usd_day,
        cfg.loss_stop_usd,
        w.log_path
    );
    let sizing = Sizing { qty: cfg.qty, cap: cfg.cap, min_lead: cfg.min_lead };
    let reduce_only = Sizing { cap: 0, ..sizing };
    let mut quoted: BTreeSet<String> = BTreeSet::new();
    let mut reduce: BTreeSet<String> = BTreeSet::new();
    let mut windows: Result<Vec<Window>, String> = Ok(Vec::new());
    let mut windows_at: Option<Instant> = None;
    let mut wait = Duration::from_secs(1);
    loop {
        let fills = tokio::time::timeout(wait, dirty()).await.unwrap_or_default();
        if windows_at.is_none_or(|t| t.elapsed() >= BLACKOUT_REREAD) {
            windows = crate::maker_exit::read_entry_blackouts(&w.blackouts_path);
            windows_at = Some(Instant::now());
        }
        let books: BTreeMap<String, Seen> = BOOKS
            .lock()
            .map(|mut g| {
                let taken = g.clone();
                g.values_mut().for_each(|seen| seen.drops = Default::default());
                taken
            })
            .unwrap_or_default();
        let killed = exists(&w.kill_file);
        let now = now_wall();

        // Settle fills first, so every decision below sees the position as it is.
        for (name, m) in mkts.iter_mut() {
            let frames = fills.get(name).cloned().unwrap_or_default();
            for side in [true, false] {
                let before = m.book.pos;
                check_fills(m, side, &frames, &w).await;
                if m.book.pos != before {
                    match (if side { &mut m.bid } else { &mut m.ask }).as_mut() {
                        // Whatever stood ahead of a filled order has traded.
                        Some(o) if o.resting() > 0 => o.ahead = 0.0,
                        // Filled whole, mid-sweep: the side waits before it adds again.
                        _ => m.last_touch[usize::from(!side)] = Some(Instant::now()),
                    }
                }
            }
            settle_cancelled(m, &w).await;
            resolve_lost(m, &w).await;
        }

        // Marks, adoptions, and what each market's book would model.
        for (name, m) in mkts.iter_mut() {
            let Some(Seen { sides: (bids, asks), .. }) = books.get(name) else {
                m.modelled = 0.0;
                continue;
            };
            let (bo, ao) = others_of(m, bids, asks, w.shadow);
            // Marked between the touches, not the best prices: a 1-lot inside
            // the spread would move the mark, and the loss stop with it.
            if let Some(mid) = mark_mid(&bo, &ao, cfg.min_lead) {
                m.mid = Some(mid);
            }
            if let (true, Some(mid)) = (m.unadopted != 0, m.mid) {
                let n = m.unadopted;
                m.book.pos += n;
                m.book.cash_e4 -= mid * n;
                m.unadopted = 0;
                append(&w, json!({"ts": now, "event": "adopt", "market": name, "qty": n, "price": px_str(mid)}));
                eprintln!("[lip] {name}: ADOPTED {n} contract(s) the log did not account for, at mid {}", px_str(mid));
            }
            let wt = want(&bo, &ao, m.book.pos, &sizing);
            m.model.push(modelled_share(&bo, &ao, wt.bid, wt.ask) * m.cfg.usd_day, now);
            m.modelled = m.model.get();
        }

        // The loss stop, over every market's inventory at its last mid, BEFORE
        // anything is placed on this pass. Removing the stop file resumes only
        // if P&L is back above the stop; otherwise the next pass writes it again.
        // A lost place or an unreadable final count can have written it too.
        let pnl: f64 = mkts.values().map(|m| m.mid.map_or(0.0, |mid| m.book.pnl_at(mid))).sum();
        if !stopped(&w) && pnl <= -cfg.loss_stop_usd {
            write_stop(&w, &format!("LOSS STOP: inventory P&L ${pnl:.2} reached the ${:.0} stop", cfg.loss_stop_usd));
        }
        let stopped = stopped(&w);
        let budget_spent = places_today(now) >= cfg.max_places_day;

        // Which markets may rest at all.
        let mut cands = Vec::new();
        let mut why_not: BTreeMap<String, String> = BTreeMap::new();
        for (name, m) in &mkts {
            let blocked = if killed {
                Some("kill file".to_string())
            } else if stopped {
                Some("stopped (see the stop file)".to_string())
            } else if quiet(&cfg.quiet_utc, now) {
                Some("quiet window".to_string())
            } else if budget_spent {
                Some(format!("today's {} places are spent", cfg.max_places_day))
            } else if let Some(r) = &m.refused {
                Some(r.clone())
            } else if !m.lost.is_empty() {
                Some("a place whose answer was lost is being looked for".to_string())
            } else if !books.contains_key(name) {
                Some("no trusted book".to_string())
            } else {
                match &windows {
                    Err(e) => Some(format!("blackout file unreadable: {e}")),
                    Ok(ws) => w
                        .rels
                        .get(name)
                        .into_iter()
                        .flatten()
                        .find(|r| ws.iter().any(|win| crate::maker_exit::window_holds(win, r, now)))
                        .map(|r| format!("entry blackout on {r}")),
                }
            };
            let over = (now >= m.until)
                .then(|| if m.cfg.until.is_empty() { "held, not configured" } else { "program over" }.to_string());
            cands.push(Cand {
                market: name.clone(),
                usd_day: m.modelled,
                quoted: quoted.contains(name),
                held: m.held(),
                open: blocked.is_none() && over.is_none(),
                exits: blocked.is_none(),
            });
            if let Some(b) = blocked.or(over) {
                why_not.insert(name.clone(), b);
            }
        }
        let chosen = select(&cands, cfg.min_usd_day, cfg.max_markets);
        if chosen != quoted {
            eprintln!(
                "[lip] quoting {:?} (modelled: {})",
                chosen,
                cands.iter().filter(|c| c.open).map(|c| format!("{} ${:.2}", c.market, c.usd_day)).collect::<Vec<_>>().join(", ")
            );
        }
        let shrink = reducing(&cands, &chosen);
        if shrink != reduce {
            eprintln!("[lip] reducing only {shrink:?} (held, not quoted: each rests the side that shrinks its position)");
        }
        quoted = chosen;
        reduce = shrink;

        // Act. Cancel before place, per side.
        let mut pending = false;
        for (name, m) in mkts.iter_mut() {
            let sz = if quoted.contains(name) {
                Some(sizing)
            } else if reduce.contains(name) {
                Some(reduce_only)
            } else {
                None
            };
            let seen = books.get(name).cloned().unwrap_or_else(|| {
                // The book was lost, and with it the sight of what left the queue.
                for o in [m.bid.as_mut(), m.ask.as_mut()].into_iter().flatten() {
                    o.ahead = 0.0;
                }
                Seen::default()
            });
            let (bo, ao) = others_of(m, &seen.sides.0, &seen.sides.1, w.shadow);
            let wt = sz.map_or(Want::default(), |s| want(&bo, &ao, m.book.pos, &s));
            for side in [true, false] {
                let (wanted, own) = if side { (wt.bid, &bo) } else { (wt.ask, &ao) };
                pending |= act_side(m, side, wanted, own, &seen.drops[usize::from(!side)], sz.as_ref(), &w).await;
            }
            pending |= !m.unsettled.is_empty() || !m.lost.is_empty();
        }

        publish(&mkts, &quoted, &reduce, &why_not, pnl, stopped, w.shadow);
        wait = if pending { MIN_REQUOTE } else { Duration::from_secs(30) };
    }
}

/// Both sides of the book without our own orders in them. A shadow order never
/// reached the venue, so there is nothing of ours to take out.
fn others_of(m: &Mkt, bids: &[Level], asks: &[Level], shadow: bool) -> (Levels, Levels) {
    let own = |o: &Option<Order>| if shadow { None } else { o.as_ref().map(|o| (o.price, o.resting())) };
    (others(bids, own(&m.bid)), others(asks, own(&m.ask)))
}

/// Bring one side's fills up to date: on a fill frame naming the order, and
/// on the heartbeat, which also notices an order that is no longer resting.
async fn check_fills(m: &mut Mkt, bid: bool, frames: &BTreeMap<String, i64>, w: &Wiring) {
    if w.shadow {
        return;
    }
    let Some(o) = (if bid { &m.bid } else { &m.ask }).as_ref() else { return };
    let frame = frames.get(&o.venue_id).copied();
    let named = frame.is_some();
    if !named && o.checked.elapsed() < HEARTBEAT {
        return;
    }
    let id = o.venue_id.clone();
    // The frame's count is booked before the read, which can lag it: on
    // 2026-11-11 the reads on four partial-fill frames found nothing new, and
    // those fills waited ~2 min for the heartbeat. Until a fill is booked, the
    // order is taken out of its level at its old size, so the level looks
    // emptier than it is and the order can be pulled from the touch.
    if let Some(cum) = frame {
        let market = m.cfg.market.clone();
        account(&market, &mut m.book, if bid { &mut m.bid } else { &mut m.ask }, bid, cum, w);
    }
    // Due again only at the next heartbeat or frame, whatever the reads below
    // answer: a failing read must not repeat on every book event.
    if let Some(o) = (if bid { &mut m.bid } else { &mut m.ask }).as_mut() {
        o.checked = Instant::now();
    }
    // On the heartbeat, first ask whether it is still working: a terminal
    // order reports its final count.
    let mut terminal = None;
    if !named {
        let (k, id) = (w.kalshi.clone(), id.clone());
        match blocking(move || k.terminal_filled_qty(&id).map_err(|e| e.to_string())).await {
            Ok(Ok(n)) => terminal = n,
            Ok(Err(e)) | Err(e) => {
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("[lip] {}: order status unreadable ({e})", m.cfg.market);
                return;
            }
        }
    }
    let filled = match terminal {
        Some(n) => n,
        None => {
            let k = w.kalshi.clone();
            match blocking(move || k.filled_qty(&id).map_err(|e| e.to_string())).await {
                Ok(Ok(n)) => n,
                Ok(Err(e)) | Err(e) => {
                    ERRORS.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[lip] {}: fill count unreadable ({e})", m.cfg.market);
                    return;
                }
            }
        }
    };
    let market = m.cfg.market.clone();
    let slot = if bid { &mut m.bid } else { &mut m.ask };
    account(&market, &mut m.book, slot, bid, filled, w);
    if terminal.is_some() {
        if let Some(o) = slot.take() {
            release_order(&o.venue_id);
            eprintln!(
                "[lip] {market}: {} {} @ {} is no longer resting at the venue ({} filled); re-placing",
                if bid { "bid" } else { "ask" },
                o.qty,
                px_str(o.price),
                o.filled
            );
        }
    }
}

/// Book whatever `filled` adds over what this order was already credited.
fn account(market: &str, book: &mut Book, slot: &mut Option<Order>, bid: bool, filled: i64, w: &Wiring) {
    let Some(o) = slot.as_mut() else { return };
    let n = filled - o.filled;
    if n <= 0 {
        return;
    }
    let n = n.min(o.resting());
    o.filled += n;
    book.fill(bid, o.price, n, arb_core::fees::kalshi_charges_makers(market));
    FILLED_CT.fetch_add(n as u64, Ordering::Relaxed);
    append(
        w,
        json!({
            "ts": now_wall(), "event": "fill", "market": market,
            "side": if bid { "bid" } else { "ask" }, "qty": n, "price": px_str(o.price),
            "order": o.venue_id, "pos": book.pos,
        }),
    );
    eprintln!(
        "[lip] FILL {market} {} {n} @ {} — position {} (order {})",
        if bid { "bid" } else { "ask" },
        px_str(o.price),
        book.pos,
        o.venue_id
    );
}

/// Move one side toward `want`. `own` are the other makers' levels on this
/// side, `drops` how far each of the side's prices has fallen since the last
/// pass, and `s` is `None` for a market that may rest nothing. Returns true
/// when a change is still owed, so the caller wakes again soon.
async fn act_side(
    m: &mut Mkt,
    bid: bool,
    want: Option<(i64, i64)>,
    own: &[(i64, f64)],
    drops: &BTreeMap<i64, f64>,
    s: Option<&Sizing>,
    w: &Wiring,
) -> bool {
    let i = usize::from(!bid);
    let market = m.cfg.market.clone();
    let side = if bid { "bid" } else { "ask" };
    let since = |m: &Mkt| m.last_touch[i].map_or(Duration::MAX, |t| t.elapsed());
    // The book may not show an order this fresh yet. Until it does, `own` has
    // that order's size taken out of a level it is not in, and reads short.
    let fresh = !w.shadow && since(m) < MIN_REQUOTE;
    if let Some(o) = (if bid { &mut m.bid } else { &mut m.ask }).as_mut() {
        o.track(drops);
    }
    let resting = (if bid { &m.bid } else { &m.ask }).clone();
    let mut pulled = false;
    let stepping = m.closer[i].take();
    if let Some(o) = &resting {
        let verdict = match s {
            None => Stay::Pull,
            Some(_) if fresh => {
                m.closer[i] = stepping;
                return true;
            }
            Some(s) => stay((o.price, o.resting()), o.ahead, want, own, bid, m.book.pos, s),
        };
        // A pull goes at once. A move away waits out its hold since the side
        // was last touched, so a flickering book cannot churn the order. A
        // move closer waits until that price has been wanted for its hold
        // without a break. Each waits out the backoff after a refusal.
        let ready = match (verdict, m.fails[i]) {
            (Stay::Keep, _) => return false,
            (Stay::Pull, 0) => true,
            (Stay::Pull, f) => since(m) >= backoff(f),
            (Stay::Reprice { closer: false }, f) => since(m) >= backoff(f).max(REPRICE_HOLD),
            (Stay::Reprice { closer: true }, f) => {
                let from = stepping.filter(|c| Some(c.0) == want.map(|x| x.0)).map_or_else(Instant::now, |c| c.1);
                m.closer[i] = want.map(|x| (x.0, from));
                from.elapsed() >= STEP_IN_HOLD && since(m) >= backoff(f)
            }
        };
        // An order filled whole is one the sweep took, the same as a pull.
        pulled = verdict == Stay::Pull || o.resting() == 0;
        if !ready {
            return true;
        }
        m.closer[i] = None;
        m.last_touch[i] = Some(Instant::now());
        if w.shadow {
            forget(m, bid);
        } else {
            let k = w.kalshi.clone();
            let req = CancelRequest { by: CancelBy::VenueId(o.venue_id.clone()), market_slug: None };
            if let Ok(Err(e)) | Err(e) = blocking(move || k.cancel(&req).map_err(|e| e.to_string())).await {
                m.fails[i] += 1;
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("[lip] {market}: cancel of {} failed ({e}); it stays on the books", o.venue_id);
                return true;
            }
            // Gone from the book: its final count is what it filled, including
            // any fill that raced the cancel.
            let (k, id) = (w.kalshi.clone(), o.venue_id.clone());
            let before = m.book.pos;
            match blocking(move || k.terminal_filled_qty(&id).map_err(|e| e.to_string())).await {
                Ok(Ok(Some(n))) => {
                    let slot = if bid { &mut m.bid } else { &mut m.ask };
                    account(&market, &mut m.book, slot, bid, n, w);
                    forget(m, bid);
                    m.fails[i] = 0;
                    if m.book.pos != before {
                        // `want` was sized for a position we no longer hold.
                        mark(&market, None);
                        return false;
                    }
                }
                Ok(Ok(None)) => return unsettle(m, bid, o.clone(), "the venue still reports it working"),
                Ok(Err(e)) | Err(e) => return unsettle(m, bid, o.clone(), &e),
            }
        }
        // A reprice is ONE change: the replacement goes out now, not a
        // debounce later, so the side is not left empty.
    }
    let Some((p, q)) = want else { return false };
    if !m.unsettled.is_empty() || !m.lost.is_empty() {
        return true;
    }
    // An order placed where nothing rests, and an adding order's return
    // after a pull or a whole fill, wait until the book has had STEP_IN_HOLD
    // to settle: an adding order lost what it stood behind, and a side that
    // comes and goes would otherwise spend a place on every turn. A replace,
    // and a reducing order pulled for its size, go out in the same pass.
    let adding = if bid { m.book.pos >= 0 } else { m.book.pos <= 0 };
    let wait = if resting.is_none() || adding && pulled { backoff(m.fails[i]).max(STEP_IN_HOLD) } else { Duration::ZERO };
    if since(m) < wait {
        return true;
    }
    // The pass read these before its venue calls, any of which can take
    // seconds. A kill, a stop or a dropped book since then must not be
    // answered with a fresh order.
    let book_gone = !BOOKS.lock().is_ok_and(|g| g.contains_key(&market));
    if exists(&w.kill_file) || stopped(w) || book_gone {
        mark(&market, None);
        return false;
    }
    count_place(now_wall());
    m.last_touch[i] = Some(Instant::now());
    let coid = next_coid();
    let req = PlaceRequest {
        market: market.clone(),
        side: if bid { Side::Bid } else { Side::Ask },
        price: px_str(p),
        qty: q,
        tif: Tif::Gtc,
        post_only: true,
        client_order_id: coid.clone(),
    };
    let venue_id = if w.shadow {
        format!("shadow-{coid}")
    } else {
        // Inside the shutdown latch, like the executor's places: either the
        // halt sees this place and waits for it, or this place sees the halt
        // and is never sent.
        let Some(inflight) = crate::exec::halt().enter(Some(&req)) else { return false };
        let (k, rq) = (w.kalshi.clone(), req.clone());
        let answer = blocking(move || {
            let r = k.place(&rq);
            drop(inflight);
            r
        })
        .await;
        match answer {
            Ok(Ok(id)) => {
                note_order(&id);
                m.fails[i] = 0;
                id
            }
            Ok(Err(e)) if never_rested(&e) => {
                m.fails[i] += 1;
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[lip] {market}: place {side} {q} @ {} refused ({e}); next try in {}s",
                    px_str(p),
                    backoff(m.fails[i]).as_secs()
                );
                return true;
            }
            Ok(Err(e)) => return lose(m, bid, req, &e.to_string()),
            Err(e) => return lose(m, bid, req, &e),
        }
    };
    PLACED.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[lip] {}REST {market} {side} {q} @ {} — id {venue_id} ({coid}), position {}, modelled ${:.2}/day",
        if w.shadow { "SHADOW " } else { "" },
        px_str(p),
        m.book.pos,
        m.modelled
    );
    let at = own.iter().find(|l| l.0 == p).map_or(0.0, |l| l.1);
    let o = Order { venue_id, price: p, qty: q, filled: 0, checked: Instant::now(), ahead: at };
    if bid {
        m.bid = Some(o);
    } else {
        m.ask = Some(o);
    }
    false
}

/// A cancelled order's final count could not be read: hold the market until it is.
fn unsettle(m: &mut Mkt, bid: bool, o: Order, why: &str) -> bool {
    ERRORS.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[lip] {}: {} was cancelled but its final fill count is not readable yet ({why}); \
         nothing rests here until it is",
        m.cfg.market, o.venue_id
    );
    m.unsettled.push((bid, o));
    m.settle_next = Some(Instant::now() + SETTLE_RETRY);
    forget(m, bid);
    true
}

/// A place came back without an answer: the venue may hold it.
fn lose(m: &mut Mkt, bid: bool, req: PlaceRequest, why: &str) -> bool {
    ERRORS.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[lip] {}: PLACE DID NOT COMPLETE ({why}) — a {} {} @ {} may be RESTING under \
         client_order_id {} with an id this process never learned. Nothing more rests on \
         this market until the venue's order list accounts for it.",
        req.market,
        if bid { "bid" } else { "ask" },
        req.qty,
        req.price,
        req.client_order_id
    );
    m.lost.push(Lost { bid, req, next: Instant::now() + SETTLE_RETRY, tries: 0 });
    true
}

/// Look for each lost place in the venue's order list. A found one becomes
/// that side's order, and the usual reads count its fills and decide whether
/// it stays. One not found in ten minutes writes the stop file, and the search
/// goes on every five minutes for the life of the process.
async fn resolve_lost(m: &mut Mkt, w: &Wiring) {
    let mut left = Vec::new();
    for mut l in std::mem::take(&mut m.lost) {
        if Instant::now() < l.next {
            left.push(l);
            continue;
        }
        let (k, rq) = (w.kalshi.clone(), l.req.clone());
        let found = blocking(move || k.recover_place(&rq, &HashSet::new()).map_err(|e| e.to_string())).await;
        let side = if l.bid { "bid" } else { "ask" };
        match found {
            Ok(Ok(Some(id))) => {
                note_order(&id);
                eprintln!(
                    "[lip] {}: RECOVERED the lost {side} {} as order {id}; its fills are read next pass",
                    m.cfg.market, l.req.client_order_id
                );
                let o = Order {
                    venue_id: id,
                    price: px(&l.req.price).unwrap_or(0),
                    qty: l.req.qty,
                    filled: 0,
                    // Due for the heartbeat read at once.
                    checked: Instant::now().checked_sub(HEARTBEAT).unwrap_or_else(Instant::now),
                    // Its place in the queue is not known: taken to be first.
                    ahead: 0.0,
                };
                let slot = if l.bid { &mut m.bid } else { &mut m.ask };
                if slot.is_none() {
                    *slot = Some(o);
                } else {
                    let why = format!("recovered order {} found its side already resting; CHECK BY HAND", o.venue_id);
                    eprintln!("[lip] {} GIVEN UP: {why}", m.cfg.market);
                    m.refused = Some(why);
                }
            }
            other => {
                l.tries += 1;
                if l.tries == LOST_TRIES {
                    // It may still be resting, under an id nothing here knows,
                    // and its fills would be booked nowhere. Everything stops
                    // until a human has looked; the search goes on slowly, and
                    // a find still adopts it so its fills are counted.
                    write_stop(
                        w,
                        &format!(
                            "{}: the {side} placed as client_order_id {} is not in the venue's \
                             order list after ten minutes ({}); it may be resting. CHECK BY HAND",
                            m.cfg.market,
                            l.req.client_order_id,
                            match other {
                                Ok(Err(e)) | Err(e) => e,
                                _ => "no order named".to_string(),
                            }
                        ),
                    );
                }
                l.next = Instant::now() + if l.tries >= LOST_TRIES { SLOW_RETRY } else { LOST_RETRY };
                left.push(l);
            }
        }
    }
    m.lost = left;
}

/// Read the final count of every cancelled order still owed one.
async fn settle_cancelled(m: &mut Mkt, w: &Wiring) {
    if m.unsettled.is_empty() || m.settle_next.is_some_and(|t| Instant::now() < t) {
        return;
    }
    m.settle_tries += 1;
    let slow = m.settle_tries > SETTLE_TRIES;
    m.settle_next = Some(Instant::now() + if slow { SLOW_RETRY } else { SETTLE_RETRY });
    let mut left = Vec::new();
    for (bid, o) in std::mem::take(&mut m.unsettled) {
        let (k, id) = (w.kalshi.clone(), o.venue_id.clone());
        match blocking(move || k.terminal_filled_qty(&id).map_err(|e| e.to_string())).await {
            Ok(Ok(Some(n))) => {
                let market = m.cfg.market.clone();
                let mut slot = Some(o);
                account(&market, &mut m.book, &mut slot, bid, n, w);
                if let Some(o) = slot {
                    release_order(&o.venue_id);
                }
            }
            _ => left.push((bid, o)),
        }
    }
    m.unsettled = left;
    if m.unsettled.is_empty() {
        m.settle_tries = 0;
    } else if m.settle_tries == SETTLE_TRIES {
        let ids: Vec<&str> = m.unsettled.iter().map(|(_, o)| o.venue_id.as_str()).collect();
        write_stop(
            w,
            &format!(
                "{}: the final fill count of cancelled order(s) {ids:?} is still unreadable after \
                 five minutes, so the position here is unknown; CHECK BY HAND",
                m.cfg.market
            ),
        );
    }
}

/// The side's order is off the book. Its id stays pinned while a final count
/// is still owed on it (`unsettled`).
fn forget(m: &mut Mkt, bid: bool) {
    CANCELLED.fetch_add(1, Ordering::Relaxed);
    let o = if bid { m.bid.take() } else { m.ask.take() };
    if let Some(o) = o {
        if !m.unsettled.iter().any(|(_, u)| u.venue_id == o.venue_id) {
            release_order(&o.venue_id);
        }
    }
}

fn publish(
    mkts: &BTreeMap<String, Mkt>,
    quoted: &BTreeSet<String>,
    reduce: &BTreeSet<String>,
    why_not: &BTreeMap<String, String>,
    pnl: f64,
    stopped: bool,
    shadow: bool,
) {
    let per: BTreeMap<&str, Value> = mkts
        .iter()
        .map(|(n, m)| {
            (
                n.as_str(),
                json!({
                    "pos": m.book.pos,
                    "pnl_usd": m.mid.map(|mid| (m.book.pnl_at(mid) * 100.0).round() / 100.0),
                    "modelled_usd_day": (m.modelled * 100.0).round() / 100.0,
                    "resting": [m.bid.as_ref().map(|o| format!("bid {} @ {}", o.resting(), px_str(o.price))),
                                m.ask.as_ref().map(|o| format!("ask {} @ {}", o.resting(), px_str(o.price)))],
                    "held_by": why_not.get(n),
                }),
            )
        })
        .collect();
    let v = json!({
        "shadow": shadow,
        "stopped": stopped,
        "quoting": quoted.len(),
        "reducing": reduce.len(),
        "modelled_usd_day": (mkts.values().filter(|m| quoted.contains(&m.cfg.market)).map(|m| m.modelled).sum::<f64>() * 100.0).round() / 100.0,
        "pnl_usd": (pnl * 100.0).round() / 100.0,
        "placed": PLACED.load(Ordering::Relaxed),
        "places_today": places_today(now_wall()),
        "cancelled": CANCELLED.load(Ordering::Relaxed),
        "filled_ct": FILLED_CT.load(Ordering::Relaxed),
        "errors": ERRORS.load(Ordering::Relaxed),
        "markets": per,
    });
    if let Ok(mut g) = STATS.lock() {
        *g = Some(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(p: &str, s: &str) -> Level {
        Level { price: p.into(), size: s.into() }
    }

    #[test]
    fn prices_parse_as_decimals_and_print_on_the_wire_grid() {
        assert_eq!(px("0.9600"), Some(9600));
        assert_eq!(px("0.96"), Some(9600));
        assert_eq!(px("1"), Some(10_000));
        assert_eq!(px("0.0005"), Some(5));
        assert_eq!(px("1.01"), None);
        assert_eq!(px("0.12345"), None);
        assert_eq!(px("-0.1"), None);
        assert_eq!(px_str(9600), "0.9600");
        assert_eq!(px_str(5), "0.0005");
    }

    #[test]
    fn our_own_order_is_taken_out_of_the_level_it_rests_in() {
        let bids = [lv("0.96", "472"), lv("0.95", "100")];
        assert_eq!(others(&bids, Some((9600, 200))), vec![(9600, 272.0), (9500, 100.0)]);
        // Alone at the top: the best OTHER bid is the next level down.
        assert_eq!(others(&bids, Some((9600, 472))), vec![(9500, 100.0)]);
        assert_eq!(others(&bids, None).len(), 2);
    }

    #[test]
    fn a_two_hundred_lot_at_the_touch_is_its_own_reference() {
        // 300 at 0.95 behind us. Our 200 at 0.96 reaches TARGET/5 alone, so
        // the 0.95 level is one tick worse and scores half.
        let side = [(9500, 300.0), (9400, 1000.0)];
        let s = side_share(&side, Some((9600, 200.0)), true).unwrap();
        // counted: 200@x1 + 300@x0.5 + 1000@x0.25 (cum hits 1000 at 0.94)
        let want = 200.0 / (200.0 + 150.0 + 250.0);
        assert!((s - want).abs() < 1e-12, "{s} vs {want}");
        // A 100-lot does not reach the reference alone; the 0.95 level becomes
        // it and scores in full.
        let s100 = side_share(&side, Some((9600, 100.0)), true).unwrap();
        assert!(s100 < 100.0 / 400.0 + 1e-12 && s100 > 0.0);
    }

    #[test]
    fn a_side_short_of_target_scores_nothing_and_so_does_the_snapshot() {
        assert_eq!(side_share(&[(9500, 500.0)], Some((9600, 200.0)), true), None);
        let deep = [(9500, 2000.0)];
        assert_eq!(modelled_share(&deep, &[(9900, 100.0)], Some((9600, 200)), Some((9900, 200))), 0.0);
        assert!(modelled_share(&deep, &[(9900, 2000.0)], Some((9600, 200)), Some((9900, 200))) > 0.0);
    }

    #[test]
    fn asks_discount_in_the_other_direction() {
        let side = [(9900, 2000.0)];
        let joined = side_share(&side, Some((9900, 200.0)), false).unwrap();
        assert!((joined - 200.0 / 2200.0).abs() < 1e-12);
        // One tick worse than the reference, we score half weight; the level
        // two ticks worse scores a quarter.
        let behind = side_share(&[(9800, 500.0), (10_000, 1000.0)], Some((9900, 200.0)), false).unwrap();
        assert!((behind - 100.0 / 850.0).abs() < 1e-12, "{behind}");
        // Behind a level that already fills the target, we are not counted.
        assert_eq!(side_share(&[(9800, 1000.0)], Some((9900, 200.0)), false), Some(0.0));
    }

    /// qty 200, cap 100, and a touch of 20 contracts.
    const SZ: Sizing = Sizing { qty: 200, cap: 100, min_lead: 20 };
    /// The same, for a market that only reduces.
    const ONLY: Sizing = Sizing { cap: 0, ..SZ };

    #[test]
    fn an_adding_order_joins_the_reference_level_others_make_and_never_makes_one() {
        // A thin side: 150 at the best bid, 17 cents of air, then 120.
        let bids = [(8800, 150.0), (7100, 120.0), (7000, 20.0), (6000, 25.0), (3200, 180.0), (2300, 15000.0)];
        let asks = [(9700, 48.0), (9800, 556.0), (9900, 2000.0)];
        let w = want(&bids, &asks, 0, &SZ);
        assert_eq!(w, Want { bid: Some((7100, 100)), ask: Some((9800, 100)) });
        // It scores in full there, and the reference level is where it was:
        // 370 at or above it, and the 20 a tick behind count half.
        let share = |o: (i64, f64)| side_share(&bids, Some(o), true).unwrap();
        assert!((share((7100, 100.0)) - 100.0 / 380.0).abs() < 1e-3);
        // Five cents under the best bid, a 200-lot models twice that, for as
        // long as the size behind it stays where it is.
        assert!(share((8300, 200.0)) > 2.0 * share((7100, 100.0)));
        // A level that is itself the reference level is joined where it is.
        let thick = [(2800, 2050.0), (2700, 2006.0)];
        assert_eq!(want(&thick, &[(2900, 2229.0)], 0, &SZ).bid, Some((2800, 100)));
        // No reference level among the others: nothing to join, and no order.
        assert_eq!(want(&bids[..1], &asks, 0, &SZ).bid, None);
        assert_eq!(want(&[], &asks, 0, &SZ).bid, None);
        assert_eq!(want(&[(9800, 300.0)], &asks, 0, &SZ), Want::default(), "crossed: nothing");
    }

    #[test]
    fn an_order_never_fills_past_the_cap() {
        let b = [(8800, 151.0), (6000, 25.0), (4600, 1152.0)];
        let a = [(9700, 48.0), (9800, 2056.0)];
        let size = |pos: i64, s: &Sizing| want(&b, &a, pos, s).bid.map(|x| x.1);
        assert_eq!(size(0, &SZ), Some(100));
        assert_eq!(size(60, &SZ), Some(40));
        assert_eq!(size(100, &SZ), None, "long at the cap: no more bids");
        assert_eq!(size(0, &Sizing { cap: 500, ..SZ }), Some(200), "never more than qty");
        assert_eq!(want(&b, &a, -100, &SZ).ask, None, "short at the cap: no more asks");
        assert_eq!(want(&b, &a, -40, &SZ).ask.map(|x| x.1), Some(60));
    }

    #[test]
    fn a_reducing_order_rests_at_the_first_real_size_not_a_one_lot_inside_it() {
        // A book its regular makers have left: 1-lots stand at 0.65 and 0.76,
        // the ask one nine cents under the first sellers with any size.
        let bids = [(6500, 1.0), (6000, 26.0), (4400, 60.0), (4300, 1557.0)];
        let asks = [(7600, 1.0), (8500, 22.0), (8600, 59.0), (8700, 568.0), (8800, 50.0)];
        assert_eq!(lead(&asks, 0.0), Some(7600), "unfiltered, the 1-lot leads");
        assert_eq!((lead(&bids, 20.0), lead(&asks, 20.0)), (Some(6000), Some(8500)));
        assert_eq!(want(&bids, &asks, 100, &ONLY), Want { bid: None, ask: Some((8500, 100)) });
        assert_eq!(want(&bids, &asks, -40, &ONLY), Want { bid: Some((6000, 40)), ask: None });
        assert_eq!(want(&bids, &asks, 300, &ONLY).ask.map(|x| x.1), Some(200), "never more than qty");
        assert_eq!(want(&bids, &asks, 0, &ONLY), Want::default(), "flat: nothing");
        assert_eq!(want(&bids, &[(7600, 1.0), (8500, 5.0)], 100, &ONLY).ask, None, "a side of dust has no touch");
        // Quoted and long: the ask is the same reducing order, sized to the
        // position, so it cannot flip it short.
        assert_eq!(want(&bids, &asks, 100, &SZ).ask, Some((8500, 100)));
    }

    #[test]
    fn a_buried_reducing_order_steps_one_tick_inside_and_stays_under_the_reference_size() {
        // A thousand contracts at the 0.92 ask, with the bid at 0.82.
        let bids = [(8200, 26.0), (6000, 25.0), (4400, 60.0), (4300, 27.0), (4000, 1141.0)];
        let asks = [(9200, 1000.0), (9300, 1266.0), (9500, 300.0)];
        assert_eq!(want(&bids, &asks, 100, &ONLY).ask, Some((9100, 100)));
        assert_eq!(want(&bids, &asks, -40, &ONLY).bid, Some((8200, 40)), "26 ahead is no queue to jump");
        // No room: one tick inside would leave no tick between it and the bid.
        assert_eq!(want(&[(9000, 30.0)], &asks, 100, &ONLY).ask, Some((9200, 100)));
        // 200 at 0.91 would be the reference level, and the thousand would
        // join it. 199 leaves the reference level at 0.92.
        assert_eq!(want(&bids, &asks, 200, &ONLY).ask, Some((9100, 199)));
        // With 15 already inside the touch, 184 is the most that does.
        let dusted = [(9000, 15.0), (9200, 1000.0), (9300, 1266.0)];
        assert_eq!(want(&bids, &dusted, 200, &ONLY).ask, Some((9100, 184)));
        // At the edge of the price range there is no tick inside: a strike
        // that has died is offered at 0.01, or bid at 0.99, with nothing on
        // the other side.
        assert_eq!(want(&[], &[(100, 1000.0)], 100, &ONLY).ask, Some((100, 100)));
        assert_eq!(want(&[(9900, 1000.0)], &[], -100, &ONLY).bid, Some((9900, 100)));
        assert_eq!(want(&[], &[(200, 1000.0)], 100, &ONLY).ask, Some((100, 100)), "0.01 is a price");
    }

    #[test]
    fn an_adding_order_is_pulled_once_too_little_would_fill_before_it() {
        let bids = [(8800, 151.0), (6100, 13.0), (6000, 25.0), (4600, 1152.0)];
        let asks = [(9700, 48.0), (9800, 2056.0)];
        let st = |r: (i64, i64), ahead: f64, b: &[(i64, f64)], pos: i64| stay(r, ahead, want(b, &asks, pos, &SZ).bid, b, true, pos, &SZ);
        // Joined at 0.46 behind 1,152: 1,341 contracts fill first.
        assert_eq!(cover(4600, 1152.0, &bids, true), 1341.0);
        assert_eq!(st((4600, 100), 1152.0, &bids, 0), Stay::Keep);
        assert_eq!(st((4600, 49), 1152.0, &bids, 0), Stay::Reprice { closer: false }, "under half is topped up");
        // The 1,152 left and another 1,152 came: the level reads the same, and
        // this order is first in it. 189 at better prices is not enough.
        assert_eq!(st((4600, 100), 0.0, &bids, 0), Stay::Pull);
        assert_eq!(st((4600, 100), 11.0, &bids, 0), Stay::Keep, "189 + 11 is");
        // A reference level made ahead of it: followed, if it lasts.
        let stepped = [(8800, 151.0), (8000, 300.0), (4600, 1152.0)];
        assert_eq!(st((4600, 100), 1152.0, &stepped, 0), Stay::Reprice { closer: true });
        // The level it joined went, with nothing better to stand behind.
        assert_eq!(st((8000, 100), 300.0, &bids[1..], 0), Stay::Pull);
        // Long 60 leaves room for 40 under the cap, and this order is for
        // 100. Or nothing is wanted.
        assert_eq!(st((4600, 100), 1152.0, &bids, 60), Stay::Pull);
        assert_eq!(stay((4600, 100), 1152.0, None, &bids, true, 0, &SZ), Stay::Pull);
    }

    #[test]
    fn a_reducing_order_follows_the_touch_and_never_outgrows_the_position() {
        let bids = [(8200, 26.0), (6000, 25.0), (4500, 1775.0)];
        let asks = [(9700, 48.0), (9800, 2056.0)];
        let at = |r: (i64, i64), ahead: f64, a: &[(i64, f64)], pos: i64| stay(r, ahead, want(&bids, a, pos, &ONLY).ask, a, false, pos, &ONLY);
        let st = |r: (i64, i64), a: &[(i64, f64)], pos: i64| at(r, 0.0, a, pos);
        assert_eq!(st((9700, 100), &asks, 100), Stay::Keep, "first in the queue is where it wants to be");
        assert_eq!(st((9700, 100), &asks, 60), Stay::Pull, "would sell 40 more than are held");
        assert_eq!(st((9700, 40), &asks, 100), Stay::Reprice { closer: false }, "under half is topped up");
        // The touch backed away: follow it out. It came closer: follow it in,
        // if it lasts.
        assert_eq!(st((9500, 100), &asks, 100), Stay::Reprice { closer: false });
        assert_eq!(st((9800, 100), &asks, 100), Stay::Reprice { closer: true });
        // One tick inside the touch it stays, buried or not.
        assert_eq!(st((9600, 100), &asks, 100), Stay::Keep);
        let buried = [(9700, 1000.0), (9800, 2056.0)];
        assert_eq!(want(&bids, &buried, 100, &ONLY).ask, Some((9600, 100)), "a new order would join the back of the thousand");
        assert_eq!(st((9600, 100), &buried, 100), Stay::Keep);
        // At the touch, it steps inside only while what is AHEAD of it buries
        // it. A thousand that joined behind an order first in the queue, or
        // behind the 150 it joined, are no reason to give up a cent.
        assert_eq!(at((9700, 100), 1000.0, &buried, 100), Stay::Reprice { closer: true });
        assert_eq!(at((9700, 100), 500.0, &buried, 100), Stay::Reprice { closer: true });
        assert_eq!(at((9700, 100), 150.0, &buried, 100), Stay::Keep);
        assert_eq!(at((9700, 100), 0.0, &buried, 100), Stay::Keep);
        // Nor does what was ahead still count once the level is smaller than it.
        assert_eq!(at((9700, 100), 1000.0, &[(9700, 300.0), (9800, 2056.0)], 100), Stay::Keep);
        // A 4-lot is buried by any touch, 20 contracts being what makes one.
        // It steps inside once and stays there when the next order joins it.
        let small = [(9700, 20.0), (9800, 2056.0)];
        assert_eq!(want(&bids, &small, 4, &ONLY).ask, Some((9600, 4)));
        let joined = [(9600, 200.0), (9700, 20.0), (9800, 2056.0)];
        assert_eq!(want(&bids, &joined, 4, &ONLY).ask, Some((9500, 4)), "a new order would step in again");
        assert_eq!(at((9600, 4), 0.0, &joined, 4), Stay::Keep, "this one was there first");
    }

    #[test]
    fn the_mark_reads_a_thin_side_at_its_best_price_and_waits_on_an_empty_one() {
        let asks = [(9200, 1.0), (9500, 300.0)];
        assert_eq!(mark_mid(&[(8800, 2.0), (8200, 26.0)], &asks, 20), Some(8850), "between the touches, not the 1-lots");
        // The bids thin out to 13 contracts in all: no touch, and the mark
        // follows them down all the same.
        assert_eq!(mark_mid(&[(6000, 3.0), (4000, 10.0)], &asks, 20), Some(7750));
        assert_eq!(mark_mid(&[], &asks, 20), None);
        assert_eq!(mark_mid(&[(8200, 26.0)], &[], 20), None);
    }

    #[test]
    fn every_fall_in_the_size_at_an_orders_price_is_taken_to_be_ahead_of_it() {
        let name = "KXA-DROPS";
        let push = |z: Option<&str>| {
            let bids: Vec<Level> = [Some(lv("0.88", "151")), z.map(|z| lv("0.46", z))].into_iter().flatten().collect();
            let asks = [lv("0.97", "48")];
            book_changed(name, Some((bids.as_slice(), asks.as_slice())));
        };
        let take = || std::mem::take(&mut BOOKS.lock().unwrap().get_mut(name).unwrap().drops);
        push(Some("1152"));
        let mut o = Order { venue_id: "B1".into(), price: 4600, qty: 100, filled: 0, checked: Instant::now(), ahead: 1152.0 };
        push(Some("1500"));
        o.track(&take()[0]);
        assert_eq!(o.ahead, 1152.0, "what joins is behind it");
        push(Some("1100"));
        o.track(&take()[0]);
        assert_eq!(o.ahead, 752.0, "400 left: taken from ahead");
        // The 1,100 are cancelled and put back before the loop looks again.
        // The level reads as it did, and this order is now first in it.
        push(None);
        push(Some("1100"));
        let drops = take();
        assert_eq!((drops[0].get(&4600), drops[1].len()), (Some(&1100.0), 0));
        o.track(&drops[0]);
        assert_eq!(o.ahead, 0.0);
        push(Some("2000"));
        o.track(&take()[0]);
        assert_eq!(o.ahead, 0.0, "and nothing that comes later is ahead");
        // A book that is lost and comes back starts over.
        push(Some("500"));
        book_changed(name, None);
        push(Some("100"));
        assert!(take()[0].is_empty());
    }

    #[test]
    fn quiet_windows_are_daily_utc_and_may_wrap_midnight() {
        let w = |a: &str, b: &str| vec![[a.to_string(), b.to_string()]];
        let at = |h: u32, m: u32| (20_000 * 86_400 + h * 3600 + m * 60) as f64;
        // 03:00-06:00 ET in winter.
        let gas = w("08:00", "11:00");
        assert!(!quiet(&gas, at(7, 59)) && quiet(&gas, at(8, 0)) && quiet(&gas, at(10, 59)) && !quiet(&gas, at(11, 0)));
        let wrap = w("23:30", "00:30");
        assert!(quiet(&wrap, at(23, 45)) && quiet(&wrap, at(0, 10)) && !quiet(&wrap, at(12, 0)));
        assert!(!quiet(&[], at(9, 0)));
        assert_eq!((hhmm("08:00"), hhmm("8:00"), hhmm("24:00"), hhmm("0800")), (Some(28_800), None, None, None));
    }

    fn names(s: &[&str]) -> BTreeSet<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    fn cand(m: &str, v: f64, quoted: bool, held: bool, open: bool) -> Cand {
        Cand { market: m.into(), usd_day: v, quoted, held, open, exits: open }
    }

    #[test]
    fn held_inventory_left_unquoted_rests_only_the_side_that_shrinks_it() {
        // A held market whose model fell below the floor used to drop out of
        // the quoted set and rest nothing, leaving its inventory with no
        // order to sell it back.
        let c = [cand("H", 1.37, true, true, true), cand("A", 5.0, false, false, true)];
        let q = select(&c, 3.0, 4);
        assert_eq!(q, names(&["A"]));
        assert_eq!(reducing(&c, &q), names(&["H"]));
        // Past the program's end, or with no config line, it is not quoted but still reduces.
        let over = Cand { exits: true, ..cand("H", 9.0, true, true, false) };
        assert!(select(std::slice::from_ref(&over), 3.0, 4).is_empty());
        assert_eq!(reducing(std::slice::from_ref(&over), &BTreeSet::new()), names(&["H"]));
        // A kill, stop, blackout or missing book stops the reducing side too.
        assert!(reducing(&[cand("H", 9.0, false, true, false)], &BTreeSet::new()).is_empty());
        // A quoted market already rests both sides; a flat one has nothing to shrink.
        assert!(reducing(&[cand("H", 9.0, true, true, true)], &names(&["H"])).is_empty());
        assert!(reducing(&[cand("A", 1.0, false, false, true)], &BTreeSet::new()).is_empty());
    }

    #[test]
    fn selection_takes_the_best_and_holds_what_it_quotes_against_near_ties() {
        let c = |m: &str, v: f64, now: bool| cand(m, v, now, false, true);
        let s = select(&[c("A", 5.0, false), c("B", 3.0, false), c("C", 1.0, false)], 2.0, 2);
        assert_eq!(s, names(&["A", "B"]));
        // B is quoted; C models a little better but not STICKY times better.
        let s = select(&[c("A", 5.0, true), c("B", 3.0, true), c("C", 3.5, false)], 2.0, 2);
        assert!(s.contains("B") && !s.contains("C"));
        // A quoted market keeps going down to min/STICKY, a new one does not start.
        assert!(select(&[c("A", 1.7, true)], 2.0, 2).contains("A"));
        assert!(select(&[c("A", 1.7, false)], 2.0, 2).is_empty());
        // A blocked market is never quoted.
        assert!(select(&[cand("A", 9.0, true, false, false)], 2.0, 2).is_empty());
    }

    #[test]
    fn every_market_holding_inventory_takes_a_slot_quotable_or_not() {
        let held = |m: &str, v: f64, open: bool| cand(m, v, false, true, open);
        let new = |m: &str, v: f64| cand(m, v, false, false, true);
        // A held market that cannot be quoted still takes one of two slots.
        assert_eq!(select(&[held("H", 0.0, false), new("A", 5.0), new("B", 4.0)], 2.0, 2), names(&["A"]));
        // A held market that can be quoted is, ahead of a better new one.
        assert_eq!(select(&[held("H", 1.7, true), new("A", 5.0)], 2.0, 1), names(&["H"]));
        // As many held markets as slots: no new market starts.
        assert!(select(&[held("H1", 0.0, false), held("H2", 0.0, false), new("A", 9.0)], 2.0, 2).is_empty());
    }

    #[test]
    fn markets_are_picked_on_a_time_mean_that_a_flickering_competitor_cannot_flip() {
        // Gas-min 3.75 on 2026-11-11: $3.34 a day without a competitor's
        // 144-lot one tick behind, $2.16 with it, swapping every ~15 s. Quoted,
        // the market must stay above min / STICKY (2.40 for a 3.00 floor).
        let mut m = Mean::default();
        let mut lowest = f64::MAX;
        for i in 0..240 {
            m.push(if i % 2 == 0 { 3.34 } else { 2.16 }, i as f64 * 14.5);
            lowest = lowest.min(m.get());
            assert!(select(&[cand("A", m.get(), true, false, true)], 3.0, 4).contains("A"), "pass {i}");
        }
        assert!((m.get() - 2.75).abs() < 0.05, "settles at the time mean, got {}", m.get());
        assert!(lowest > 2.6, "never near the floor, got {lowest}");
        // Unquoted, that mean does not clear the $3 needed to start.
        assert!(select(&[cand("A", m.get(), false, false, true)], 3.0, 4).is_empty());
        // A one-second spike barely moves it.
        let t = 240.0 * 14.5;
        let before = m.get();
        m.push(9.0, t);
        m.push(before, t + 1.0);
        assert!((m.get() - before).abs() < 0.02);
        // A real fall to $1 is below the $2.40 floor within fifteen minutes.
        m.push(1.0, t + 2.0);
        m.push(1.0, t + 2.0 + 900.0);
        assert!(m.get() < 2.4, "got {}", m.get());
        // The first value is taken as it is.
        let mut f = Mean::default();
        f.push(4.0, 100.0);
        assert_eq!(f.get(), 4.0);
    }

    /// A venue whose per-order read still shows nothing filled.
    struct StaleReads;

    impl OrderSink for StaleReads {
        fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
            unreachable!("check_fills never places")
        }
        fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
            unreachable!("check_fills never cancels")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            unreachable!("check_fills never sweeps")
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            unreachable!("check_fills never lists orders")
        }
        fn filled_qty(&self, _id: &str) -> Result<i64, VenueError> {
            Ok(0)
        }
    }

    /// A venue that takes every cancel, with nothing filled.
    struct Quiet;

    impl OrderSink for Quiet {
        fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
            unreachable!("nothing here is placed at the venue")
        }
        fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
            Ok(())
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            unreachable!("act_side never sweeps")
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            unreachable!("act_side never lists orders")
        }
        fn terminal_filled_qty(&self, _id: &str) -> Result<Option<i64>, VenueError> {
            Ok(Some(0))
        }
    }

    fn wiring(kalshi: Arc<dyn OrderSink>, shadow: bool) -> Wiring {
        Wiring { kalshi, log_path: String::new(), blackouts_path: String::new(), kill_file: String::new(), rels: BTreeMap::new(), shadow }
    }

    /// A flat market with a trusted book, resting nothing.
    fn mkt(name: &str) -> Mkt {
        BOOKS.lock().unwrap().insert(name.into(), Default::default());
        Mkt {
            cfg: MarketCfg { market: name.into(), usd_day: 5.0, until: String::new() },
            until: f64::MAX,
            bid: None,
            ask: None,
            last_touch: [None, None],
            fails: [0, 0],
            closer: [None, None],
            book: Book::default(),
            unadopted: 0,
            mid: None,
            modelled: 0.0,
            model: Mean::default(),
            unsettled: Vec::new(),
            settle_next: None,
            settle_tries: 0,
            lost: Vec::new(),
            refused: None,
        }
    }

    fn resting(o: &Option<Order>) -> Option<(i64, i64, f64)> {
        o.as_ref().map(|o| (o.price, o.resting(), o.ahead))
    }

    #[tokio::test]
    async fn a_pulled_adding_order_waits_before_it_joins_again_and_a_resized_reducing_one_does_not() {
        let w = wiring(Arc::new(StaleReads), true);
        let mut m = mkt("KXA-REJOIN");
        let none = BTreeMap::new();
        let asks = [(9700, 48.0), (9800, 2056.0)];
        let bids = |z: f64| [(8800, 151.0), (6000, 38.0), (4600, z)];
        // Joins the 1,152 at 0.46, behind them.
        let b = bids(1152.0);
        act_side(&mut m, true, want(&b, &asks, 0, &SZ).bid, &b, &none, Some(&SZ), &w).await;
        assert_eq!(resting(&m.bid), Some((4600, 100, 1152.0)));
        // Another 1,152 join behind it: nothing changes for this order.
        let b = bids(2304.0);
        assert!(!act_side(&mut m, true, want(&b, &asks, 0, &SZ).bid, &b, &none, Some(&SZ), &w).await);
        assert_eq!(resting(&m.bid), Some((4600, 100, 1152.0)));
        // 1,152 leave and as many come back before the loop looks again. The
        // level reads as it did and is still the reference level, and this
        // order is now first in it: pulled, and not put back yet.
        let left = BTreeMap::from([(4600, 1152.0)]);
        assert_eq!(want(&b, &asks, 0, &SZ).bid, Some((4600, 100)));
        assert!(act_side(&mut m, true, want(&b, &asks, 0, &SZ).bid, &b, &left, Some(&SZ), &w).await);
        assert!(act_side(&mut m, true, want(&b, &asks, 0, &SZ).bid, &b, &none, Some(&SZ), &w).await);
        assert_eq!(resting(&m.bid), None);
        // After the hold it joins again, behind what is there now.
        m.last_touch[0] = Instant::now().checked_sub(STEP_IN_HOLD);
        act_side(&mut m, true, want(&b, &asks, 0, &SZ).bid, &b, &none, Some(&SZ), &w).await;
        assert_eq!(resting(&m.bid), Some((4600, 100, 2304.0)));
        // An order that sells inventory back is resized in the same pass.
        m.book.pos = 100;
        act_side(&mut m, false, want(&b, &asks, 100, &SZ).ask, &asks, &none, Some(&SZ), &w).await;
        assert_eq!(resting(&m.ask), Some((9700, 100, 48.0)));
        m.book.pos = 60;
        act_side(&mut m, false, want(&b, &asks, 60, &SZ).ask, &asks, &none, Some(&SZ), &w).await;
        assert_eq!(resting(&m.ask), Some((9700, 60, 48.0)));
        // A market that may rest nothing pulls both, whatever the book says.
        act_side(&mut m, true, None, &b, &none, None, &w).await;
        act_side(&mut m, false, None, &asks, &none, None, &w).await;
        assert_eq!((resting(&m.bid), resting(&m.ask)), (None, None));
        // Where nothing rests, a reducing order waits the hold out too: a side
        // that comes and goes must not cost a place each time it comes back.
        assert!(act_side(&mut m, false, want(&b, &asks, 60, &SZ).ask, &asks, &none, Some(&SZ), &w).await);
        assert_eq!(resting(&m.ask), None);
        m.last_touch[1] = Instant::now().checked_sub(STEP_IN_HOLD);
        act_side(&mut m, false, want(&b, &asks, 60, &SZ).ask, &asks, &none, Some(&SZ), &w).await;
        assert_eq!(resting(&m.ask), Some((9700, 60, 48.0)));
    }

    #[tokio::test]
    async fn a_closer_level_is_followed_only_once_it_has_lasted() {
        let w = wiring(Arc::new(StaleReads), true);
        let mut m = mkt("KXA-STEPIN");
        let none = BTreeMap::new();
        let asks = [(9700, 48.0), (9800, 2056.0)];
        let deep = [(8800, 151.0), (4600, 1152.0)];
        let stepped = [(8800, 151.0), (8000, 300.0), (4600, 1152.0)];
        let (at_deep, at_stepped) = (want(&deep, &asks, 0, &SZ).bid, want(&stepped, &asks, 0, &SZ).bid);
        assert_eq!((at_deep, at_stepped), (Some((4600, 100)), Some((8000, 100))));
        act_side(&mut m, true, at_deep, &deep, &none, Some(&SZ), &w).await;
        m.last_touch[0] = Instant::now().checked_sub(STEP_IN_HOLD * 10);
        assert_eq!(resting(&m.bid), Some((4600, 100, 1152.0)));
        // 300 show at 0.80, the new reference level. The order has rested for
        // ten minutes, and still does not follow at once.
        assert!(act_side(&mut m, true, at_stepped, &stepped, &none, Some(&SZ), &w).await);
        assert_eq!((resting(&m.bid), m.closer[0].map(|c| c.0)), (Some((4600, 100, 1152.0)), Some(8000)));
        // The 300 go: the clock is forgotten, and starts again when they return.
        assert!(!act_side(&mut m, true, at_deep, &deep, &none, Some(&SZ), &w).await);
        assert_eq!(m.closer[0], None);
        assert!(act_side(&mut m, true, at_stepped, &stepped, &none, Some(&SZ), &w).await);
        let since = m.closer[0].unwrap().1;
        assert!(act_side(&mut m, true, at_stepped, &stepped, &none, Some(&SZ), &w).await);
        assert_eq!(m.closer[0].unwrap().1, since, "the same wanted price keeps its clock");
        // A minute on they are still there: joined, behind them.
        m.closer[0] = Some((8000, Instant::now().checked_sub(STEP_IN_HOLD).unwrap()));
        act_side(&mut m, true, at_stepped, &stepped, &none, Some(&SZ), &w).await;
        assert_eq!((resting(&m.bid), m.closer[0]), (Some((8000, 100, 300.0)), None));
    }

    #[tokio::test]
    async fn an_order_the_book_may_not_show_yet_is_not_judged_but_a_block_still_pulls_it() {
        let w = wiring(Arc::new(Quiet), false);
        let mut m = mkt("KXA-FRESH");
        let none = BTreeMap::new();
        let asks = [(9700, 48.0), (9800, 2056.0)];
        let b = [(8800, 151.0), (4600, 1152.0)];
        let wanted = want(&b, &asks, 0, &SZ).bid;
        // Placed just now behind 1,152. Orders are put in the slot by hand:
        // a place goes through the process-wide shutdown latch, which another
        // test in this binary leaves on.
        let placed = |m: &mut Mkt, at: Option<Instant>| {
            m.bid = Some(Order { venue_id: "B1".into(), price: 4600, qty: 100, filled: 0, checked: Instant::now(), ahead: 1152.0 });
            m.last_touch[0] = at;
        };
        placed(&mut m, Some(Instant::now()));
        // The feed has not caught up: the level reads 100 short, as if this
        // order's size had left it, and under the reference size. Not judged.
        let lagging = [(8800, 51.0), (4600, 1052.0)];
        assert_eq!(stay((4600, 100), 0.0, wanted, &lagging, true, 0, &SZ), Stay::Pull);
        let gone = BTreeMap::from([(4600, 1152.0)]);
        assert!(act_side(&mut m, true, wanted, &lagging, &gone, Some(&SZ), &w).await);
        assert_eq!(resting(&m.bid), Some((4600, 100, 0.0)), "the fall is still counted");
        // A market that may rest nothing pulls it at once all the same.
        act_side(&mut m, true, None, &b, &none, None, &w).await;
        assert_eq!(resting(&m.bid), None);
        // Once the book has had time to show it, it is judged, and pulled.
        placed(&mut m, Instant::now().checked_sub(MIN_REQUOTE));
        assert!(act_side(&mut m, true, wanted, &b, &gone, Some(&SZ), &w).await);
        assert_eq!(resting(&m.bid), None);
    }

    #[tokio::test]
    async fn a_fill_frame_is_booked_at_once_even_when_the_read_lags_it() {
        let dir = std::env::temp_dir().join(format!("lip-frame-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("lip.jsonl");
        let _ = std::fs::remove_file(&log);
        let w = Wiring {
            kalshi: Arc::new(StaleReads),
            log_path: log.to_str().unwrap().to_string(),
            blackouts_path: String::new(),
            kill_file: String::new(),
            rels: BTreeMap::new(),
            shadow: false,
        };
        let order = |id: &str| Order { venue_id: id.into(), price: 8400, qty: 100, filled: 0, checked: Instant::now(), ahead: 0.0 };
        let mut m = Mkt {
            cfg: MarketCfg { market: "KXA-1".into(), usd_day: 5.0, until: String::new() },
            until: f64::MAX,
            bid: Some(order("B1")),
            ask: Some(order("A1")),
            last_touch: [None, None],
            fails: [0, 0],
            closer: [None, None],
            book: Book::default(),
            unadopted: 0,
            mid: None,
            modelled: 0.0,
            model: Mean::default(),
            unsettled: Vec::new(),
            settle_next: None,
            settle_tries: 0,
            lost: Vec::new(),
            refused: None,
        };
        let frames = |id: &str, cum: i64| BTreeMap::from([(id.to_string(), cum)]);
        check_fills(&mut m, true, &frames("B1", 2), &w).await;
        assert_eq!(m.book.pos, 2, "booked from the frame, though the read says 0");
        assert_eq!(m.bid.as_ref().unwrap().resting(), 98);
        // Cumulative: the next frame books only what it adds, and a stale or
        // repeated frame books nothing.
        check_fills(&mut m, true, &frames("B1", 12), &w).await;
        check_fills(&mut m, true, &frames("B1", 12), &w).await;
        check_fills(&mut m, true, &frames("B1", 5), &w).await;
        assert_eq!(m.book.pos, 12);
        // A frame for the other side's order leaves this side alone.
        check_fills(&mut m, true, &frames("A1", 7), &w).await;
        assert_eq!(m.book.pos, 12);
        check_fills(&mut m, false, &frames("A1", 7), &w).await;
        assert_eq!(m.book.pos, 5);
        // Never past what the order was for.
        check_fills(&mut m, true, &frames("B1", 500), &w).await;
        assert_eq!(m.bid.as_ref().unwrap().resting(), 0);
        let fills = std::fs::read_to_string(&log).unwrap().lines().count();
        assert_eq!(fills, 4, "one log record per booking");
    }

    #[test]
    fn the_backoff_doubles_to_a_minute_and_only_certain_refusals_mean_nothing_rested() {
        assert_eq!(backoff(0), MIN_REQUOTE);
        assert_eq!(backoff(1), MIN_REQUOTE * 2);
        assert_eq!(backoff(9), MAX_BACKOFF);
        assert!(never_rested(&VenueError::Status { endpoint: "x", status: 409, body: String::new() }));
        assert!(!never_rested(&VenueError::Transport("timed out".into())));
        assert!(!never_rested(&VenueError::Status { endpoint: "x", status: 502, body: String::new() }));
    }

    #[test]
    fn a_torn_log_still_names_every_market_it_mentions() {
        let dir = std::env::temp_dir().join(format!("lip-log-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("lip.jsonl");
        let ps = p.to_str().unwrap();
        assert!(log_holdings(ps).held.is_empty() && log_holdings(ps).error.is_none(), "a missing log holds nothing");
        std::fs::write(
            &p,
            concat!(
                r#"{"event":"fill","market":"KXA-1","side":"bid","qty":5,"price":"0.5000"}"#, "\n",
                r#"{"event":"fill","market":"KXB-1","side":"bid","qty":5,"price":"0.5000"}"#, "\n",
                r#"{"event":"fill","market":"KXB-1","side":"ask","qty":5,"price":"0.5200"}"#, "\n",
                "\0\0\0{\"event\":\"fi", "\n",
            ),
        )
        .unwrap();
        let h = log_holdings(ps);
        assert_eq!(h.held, BTreeMap::from([("KXA-1".to_string(), 5)]));
        assert_eq!(h.named, names(&["KXA-1", "KXB-1"]));
        assert!(h.error.is_some(), "a torn line is reported, not skipped silently");
    }

    #[test]
    fn fills_mark_on_the_yes_axis_and_a_round_trip_keeps_the_spread() {
        let mut b = Book::default();
        b.fill(true, 9600, 200, false);
        assert_eq!((b.pos, b.cash_e4), (200, -1_920_000));
        assert!((b.pnl_at(9750) - 3.0).abs() < 1e-9, "200 bought at 0.96, marked 0.975");
        b.fill(false, 9900, 200, false);
        assert_eq!(b.pos, 0);
        assert!((b.pnl_at(5000) - 6.0).abs() < 1e-9, "the 3c spread on 200, whatever the mark");
        let mut f = Book::default();
        f.fill(true, 5000, 100, true);
        assert!((f.fees_usd - 0.0175 * 0.25 * 100.0).abs() < 1e-12);
    }

    #[test]
    fn the_log_replays_fills_and_adoptions_into_the_same_books() {
        let recs = vec![
            json!({"event": "fill", "market": "KXAAAGASMINM-26NOV30-3.70", "side": "bid", "qty": 10, "price": "0.8300"}),
            json!({"event": "fill", "market": "KXAAAGASMINM-26NOV30-3.70", "side": "ask", "qty": 4, "price": "0.8500"}),
            json!({"event": "adopt", "market": "KXAAAGASMINM-26NOV30-3.70", "qty": -2, "price": "0.8400"}),
            json!({"event": "stop", "pnl_usd": -1.0}),
        ];
        let b = replay(&recs)["KXAAAGASMINM-26NOV30-3.70"];
        assert_eq!(b.pos, 4);
        assert_eq!(b.cash_e4, -83_000 + 34_000 + 16_800);
    }

    #[test]
    fn the_config_refuses_what_it_cannot_honour() {
        let dir = std::env::temp_dir().join(format!("lip-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("lip.yaml");
        let ok = "qty: 200\ncap: 200\nmin_lead: 20\nquiet_utc: [[\"08:00\", \"11:00\"]]\nmax_markets: 5\nmin_usd_day: 2.0\nloss_stop_usd: 150\nmax_places_day: 600\nmarkets:\n  - {market: KXA-1, usd_day: 14.29, until: 2026-11-15T05:00:00Z}\n";
        std::fs::write(&p, ok).unwrap();
        assert!(load(p.to_str().unwrap()).is_ok());
        std::fs::write(&p, ok.replace("cap: 200", "cap: 100")).unwrap();
        assert!(load(p.to_str().unwrap()).is_ok(), "qty over cap is trimmed by want(), not refused");
        for bad in [
            ok.replace("qty: 200", "qty: 0"),
            ok.replace("cap: 200", "cap: 0"),
            ok.replace("2026-11-15T05:00:00Z", "next week"),
            ok.replace("loss_stop_usd: 150", "loss_stop_usd: 0"),
            ok.replace("max_markets: 5", "max_markets: 0"),
            ok.replace("max_places_day: 600", "max_places_day: 0"),
            ok.replace("max_places_day: 600\n", ""),
            ok.replace("min_lead: 20\n", ""),
            ok.replace("quiet_utc: [[\"08:00\", \"11:00\"]]\n", ""),
            ok.replace("min_lead: 20", "min_lead: -1"),
            ok.replace("11:00", "8am"),
            ok.replace("11:00", "08:00"),
            format!("{ok}  - {{market: KXA-1, usd_day: 1, until: 2026-11-15T05:00:00Z}}\n"),
            ok.replace("min_usd_day", "min_usd_per_day"),
        ] {
            std::fs::write(&p, &bad).unwrap();
            assert!(load(p.to_str().unwrap()).is_err(), "accepted:\n{bad}");
        }
    }
}
