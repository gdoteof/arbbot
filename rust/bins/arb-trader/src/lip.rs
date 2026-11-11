//! Kalshi liquidity-incentive (LIP) quoting. OFF unless `--lip <config>`.
//!
//! WHAT IT EARNS. Kalshi pays each market's incentive pool across one-second
//! snapshots of resting size near the touch. Per side, the reference price is
//! the first level where cumulative size reaches `TARGET / 5`. Levels at or
//! better than it score x1.0, and a level `n` ticks worse scores `DISCOUNT^n`.
//! A side short of `TARGET` scores nothing, and a snapshot pays only when BOTH
//! sides reach it. Every program listed on 2026-11-10 used a target of 1,000
//! and a discount of 0.5. A 200-lot at the best price is therefore its own
//! reference level, and everything resting behind it is discounted. That is why
//! thin books pay. The tape replay is in `data/scan/rewards-2026-11-10/`.
//!
//! WHAT IT DOES. For each configured Kalshi market it rests a `qty` YES bid at
//! the best bid OTHER makers show and a `qty` YES ask at the best ask they
//! show. It holds whatever fills, with no hedge. Net inventory is capped at
//! `cap` contracts either way: each order is trimmed to what keeps the position
//! inside the cap, so the side that would add past it is withdrawn. A
//! market is quoted only while its book, scored by the rule above with our two
//! orders in it, models at least `min_usd_day` averaged over about ten minutes
//! ([`MODEL_TAU_S`]). At most `max_markets` are
//! quoted at once, the best-modelled ones. The model is the rule applied to the
//! book we see; Kalshi's payout is the only check on it.
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
//!   * the market's `until`;
//!   * a place whose answer was lost, until the venue's order list accounts for it;
//!   * the loss stop.
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
    /// When the program ends, `YYYY-MM-DDTHH:MM:SSZ`. Nothing rests past it.
    pub until: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cfg {
    /// Contracts per order, before the cap trims it: an order never exceeds what
    /// keeps the position inside `cap` (see [`want`]). So `qty` may exceed `cap`:
    /// at qty 200 / cap 100 a flat market rests 100 a side, and one held at +100
    /// rests a 200 ask, which takes it to no further than -100.
    pub qty: i64,
    /// Largest net position held either way, per market.
    pub cap: i64,
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
        if reference.is_none() && cum >= TARGET / 5.0 {
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

/// The orders this market should rest: `(price, contracts)` per side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Want {
    pub bid: Option<(i64, i64)>,
    pub ask: Option<(i64, i64)>,
}

/// Join the best price other makers show on each side. Size each order so a
/// full fill cannot take the net position past `cap`, and rest nothing on the
/// side that is already there. A book whose others' best bid is at or through
/// their best ask gets nothing: post-only would refuse it, and a crossed book
/// is one we cannot see properly.
pub(crate) fn want(bids_o: &[(i64, f64)], asks_o: &[(i64, f64)], pos: i64, qty: i64, cap: i64) -> Want {
    if let (Some(b), Some(a)) = (bids_o.first(), asks_o.first()) {
        if b.0 >= a.0 {
            return Want::default();
        }
    }
    let room_long = (cap - pos).min(qty);
    let room_short = (cap + pos).min(qty);
    Want {
        bid: bids_o.first().filter(|_| room_long > 0).map(|&(p, _)| (p, room_long)),
        ask: asks_o.first().filter(|_| room_short > 0).map(|&(p, _)| (p, room_short)),
    }
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

/// Should a resting order stay? It must be at the wanted price, must not be
/// able to fill past the cap, and must have at least half its wanted size left.
/// Below half, its score is mostly gone and a fresh order restores it.
pub(crate) fn keep(resting: (i64, i64), want: Option<(i64, i64)>) -> bool {
    match want {
        Some((p, q)) => resting.0 == p && resting.1 <= q && resting.1 * 2 >= q,
        None => false,
    }
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

/// Trusted Kalshi books of the watched markets.
static BOOKS: Mutex<BTreeMap<String, Sides>> = Mutex::new(BTreeMap::new());
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
                g.insert(market.to_owned(), (b.to_vec(), a.to_vec()));
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
}

impl Order {
    fn resting(&self) -> i64 {
        self.qty - self.filled
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
    /// The last place or cancel per side (`[bid, ask]`), and the refusals
    /// since that side's last success.
    last_touch: [Option<Instant>; 2],
    fails: [u32; 2],
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
    // never quoted.
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
        "[lip] {}: {} market(s) ({} configured), {}-lots both sides at the touch, cap {} net, \
         quoting the best {} modelling >= ${:.2}/day (every market holding inventory takes a \
         slot), loss stop ${:.0}. Log {}.",
        if w.shadow { "SHADOW" } else { "ARMED" },
        mkts.len(),
        cfg.markets.len(),
        cfg.qty,
        cfg.cap,
        cfg.max_markets,
        cfg.min_usd_day,
        cfg.loss_stop_usd,
        w.log_path
    );
    let mut quoted: BTreeSet<String> = BTreeSet::new();
    let mut windows: Result<Vec<Window>, String> = Ok(Vec::new());
    let mut windows_at: Option<Instant> = None;
    let mut wait = Duration::from_secs(1);
    loop {
        let fills = tokio::time::timeout(wait, dirty()).await.unwrap_or_default();
        if windows_at.is_none_or(|t| t.elapsed() >= BLACKOUT_REREAD) {
            windows = crate::maker_exit::read_entry_blackouts(&w.blackouts_path);
            windows_at = Some(Instant::now());
        }
        let books: BTreeMap<String, Sides> = BOOKS.lock().map(|g| g.clone()).unwrap_or_default();
        let killed = exists(&w.kill_file);
        let now = now_wall();

        // Settle fills first, so every decision below sees the position as it is.
        for (name, m) in mkts.iter_mut() {
            let frames = fills.get(name).cloned().unwrap_or_default();
            for side in [true, false] {
                check_fills(m, side, &frames, &w).await;
            }
            settle_cancelled(m, &w).await;
            resolve_lost(m, &w).await;
        }

        // Marks, adoptions, and what each market's book would model.
        for (name, m) in mkts.iter_mut() {
            let Some((bids, asks)) = books.get(name) else {
                m.modelled = 0.0;
                continue;
            };
            if let (Some(b), Some(a)) = (bids.first().and_then(|l| px(&l.price)), asks.first().and_then(|l| px(&l.price))) {
                m.mid = Some((b + a) / 2);
            }
            if let (true, Some(mid)) = (m.unadopted != 0, m.mid) {
                let n = m.unadopted;
                m.book.pos += n;
                m.book.cash_e4 -= mid * n;
                m.unadopted = 0;
                append(&w, json!({"ts": now, "event": "adopt", "market": name, "qty": n, "price": px_str(mid)}));
                eprintln!("[lip] {name}: ADOPTED {n} contract(s) the log did not account for, at mid {}", px_str(mid));
            }
            let (bo, ao) = others_of(m, bids, asks, w.shadow);
            let wt = want(&bo, &ao, m.book.pos, cfg.qty, cfg.cap);
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
            } else if budget_spent {
                Some(format!("today's {} places are spent", cfg.max_places_day))
            } else if let Some(r) = &m.refused {
                Some(r.clone())
            } else if !m.lost.is_empty() {
                Some("a place whose answer was lost is being looked for".to_string())
            } else if now >= m.until {
                Some(if m.cfg.until.is_empty() { "held, not configured" } else { "program over" }.to_string())
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
            cands.push(Cand {
                market: name.clone(),
                usd_day: m.modelled,
                quoted: quoted.contains(name),
                held: m.held(),
                open: blocked.is_none(),
            });
            if let Some(b) = blocked {
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
        quoted = chosen;

        // Act. Cancel before place, per side.
        let mut pending = false;
        for (name, m) in mkts.iter_mut() {
            let wt = match (quoted.contains(name), books.get(name)) {
                (true, Some((bids, asks))) => {
                    let (bo, ao) = others_of(m, bids, asks, w.shadow);
                    want(&bo, &ao, m.book.pos, cfg.qty, cfg.cap)
                }
                _ => Want::default(),
            };
            for side in [true, false] {
                pending |= act_side(m, side, if side { wt.bid } else { wt.ask }, &w).await;
            }
            pending |= !m.unsettled.is_empty() || !m.lost.is_empty();
        }

        publish(&mkts, &quoted, &why_not, pnl, stopped, w.shadow);
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

/// Move one side toward `want`. Returns true when a change is still owed, so
/// the caller wakes again soon.
async fn act_side(m: &mut Mkt, bid: bool, want: Option<(i64, i64)>, w: &Wiring) -> bool {
    let i = usize::from(!bid);
    let market = m.cfg.market.clone();
    let side = if bid { "bid" } else { "ask" };
    let since = |m: &Mkt| m.last_touch[i].map_or(Duration::MAX, |t| t.elapsed());
    let resting = (if bid { &m.bid } else { &m.ask }).clone();
    if let Some(o) = &resting {
        if keep((o.price, o.resting()), want) {
            return false;
        }
        // A pull goes at once, and so does an order that a full fill would now
        // take past the cap. A reprice waits out REPRICE_HOLD, so a flickering
        // book cannot churn the order. Each waits out the backoff after a refusal.
        let urgent = want.is_none_or(|(_, q)| o.resting() > q);
        let gap = match (urgent, m.fails[i]) {
            (true, 0) => Duration::ZERO,
            (true, f) => backoff(f),
            (false, f) => backoff(f).max(REPRICE_HOLD),
        };
        if since(m) < gap {
            return true;
        }
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
        // debounce later, so the side is not left empty at the touch.
    }
    let Some((p, q)) = want else { return false };
    if !m.unsettled.is_empty() || !m.lost.is_empty() {
        return true;
    }
    if resting.is_none() && since(m) < backoff(m.fails[i]) {
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
    let o = Order { venue_id, price: p, qty: q, filled: 0, checked: Instant::now() };
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

    #[test]
    fn the_wanted_orders_join_the_touch_and_never_fill_past_the_cap() {
        let b = [(9600, 300.0)];
        let a = [(9900, 2000.0)];
        assert_eq!(want(&b, &a, 0, 200, 200), Want { bid: Some((9600, 200)), ask: Some((9900, 200)) });
        assert_eq!(want(&b, &a, 150, 200, 200), Want { bid: Some((9600, 50)), ask: Some((9900, 200)) });
        assert_eq!(want(&b, &a, 200, 200, 200).bid, None, "long at the cap: no more bids");
        assert_eq!(want(&b, &a, -200, 200, 200).ask, None, "short at the cap: no more asks");
        // qty over cap: flat rests the cap each side; held at the cap, the reducing side rests qty.
        assert_eq!(want(&b, &a, 0, 200, 100).bid.map(|x| x.1), Some(100));
        assert_eq!(want(&b, &a, 0, 200, 100).ask.map(|x| x.1), Some(100));
        assert_eq!(want(&b, &a, 100, 200, 100).bid, None);
        assert_eq!(want(&b, &a, 100, 200, 100).ask.map(|x| x.1), Some(200), "200 ask from +100 ends at -100");
        assert_eq!(want(&b, &a, -40, 200, 100).bid.map(|x| x.1), Some(140));
        assert_eq!(want(&[(9900, 1.0)], &a, 0, 200, 200), Want::default(), "crossed: nothing");
        assert_eq!(want(&[], &a, 0, 200, 200).bid, None, "an empty side is not led");
    }

    #[test]
    fn a_resting_order_stays_only_at_the_wanted_price_and_inside_the_cap() {
        assert!(keep((9600, 200), Some((9600, 200))));
        assert!(keep((9600, 100), Some((9600, 200))), "half left still scores");
        assert!(!keep((9600, 99), Some((9600, 200))), "under half is topped up");
        assert!(!keep((9600, 200), Some((9600, 50))), "could fill past the cap");
        assert!(!keep((9500, 200), Some((9600, 200))), "the touch moved");
        assert!(!keep((9600, 200), None));
    }

    fn names(s: &[&str]) -> BTreeSet<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    fn cand(m: &str, v: f64, quoted: bool, held: bool, open: bool) -> Cand {
        Cand { market: m.into(), usd_day: v, quoted, held, open }
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
        let order = |id: &str| Order { venue_id: id.into(), price: 8400, qty: 100, filled: 0, checked: Instant::now() };
        let mut m = Mkt {
            cfg: MarketCfg { market: "KXA-1".into(), usd_day: 5.0, until: String::new() },
            until: f64::MAX,
            bid: Some(order("B1")),
            ask: Some(order("A1")),
            last_touch: [None, None],
            fails: [0, 0],
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
        let ok = "qty: 200\ncap: 200\nmax_markets: 5\nmin_usd_day: 2.0\nloss_stop_usd: 150\nmax_places_day: 600\nmarkets:\n  - {market: KXA-1, usd_day: 14.29, until: 2026-11-15T05:00:00Z}\n";
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
            format!("{ok}  - {{market: KXA-1, usd_day: 1, until: 2026-11-15T05:00:00Z}}\n"),
            ok.replace("min_usd_day", "min_usd_per_day"),
        ] {
            std::fs::write(&p, &bad).unwrap();
            assert!(load(p.to_str().unwrap()).is_err(), "accepted:\n{bad}");
        }
    }
}
