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
//! `cap` contracts either way, by withdrawing the side that would add to it. A
//! market is quoted only while its book, scored by the rule above with our two
//! orders in it, models at least `min_usd_day`. At most `max_markets` are
//! quoted at once, the best-modelled ones. The model is the rule applied to the
//! book we see; Kalshi's payout is the only check on it.
//!
//! WHAT KEEPS IT APART FROM THE ARB ENGINE. Kalshi nets YES against NO in one
//! market, so this inventory and an arb basket's Kalshi leg cannot share a
//! market without corrupting each other. The trader therefore does three
//! things. At boot it refuses any configured market an open ledger lot holds.
//! It quotes no relationship whose Kalshi leg is configured here. And it tells
//! the positions reconciliation that those relationships' PM-US slugs are not
//! its to complete ([`owned_pm_slugs`]). Fills arrive on the engine's
//! account-wide fill feed as sidecar orders and wake this loop. The count the
//! venue reports per order is the truth.
//!
//! WHAT STOPS IT. A market rests nothing when any of these hold:
//!   * the kill file;
//!   * an untrusted or pulled feed (the engine clears the books);
//!   * an entry blackout on a relationship the market belongs to;
//!   * the market's `until`;
//!   * the loss stop.
//!
//! The loss stop fires when inventory P&L marked to mid reaches
//! `-loss_stop_usd`. It writes `<log>.STOP` and pulls everything until a human
//! removes that file.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arb_core::model::Level;
use arb_venue::gateway::{CancelBy, CancelRequest, PlaceRequest, Side, Tif};
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
    /// Contracts per order.
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
    pub markets: Vec<MarketCfg>,
}

/// Read and check the config. Every refusal names the field.
pub fn load(path: &str) -> Result<Cfg, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let cfg: Cfg = serde_yaml::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
    if cfg.qty <= 0 || cfg.cap < cfg.qty {
        return Err(format!("{path}: need 0 < qty <= cap, got qty {} cap {}", cfg.qty, cfg.cap));
    }
    if cfg.max_markets == 0 || cfg.markets.is_empty() {
        return Err(format!("{path}: no market may be quoted (max_markets 0 or no markets)"));
    }
    if !(cfg.loss_stop_usd > 0.0) || !(cfg.min_usd_day >= 0.0) {
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
        if !(m.usd_day > 0.0) {
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
pub(crate) fn others(levels: &[Level], own: Option<(i64, i64)>) -> Vec<(i64, f64)> {
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

/// Which markets to quote: eligible ones modelling at least `min` (sticky ones
/// down to `min / STICKY`), best first, at most `max`. A market already quoted
/// ranks as if it modelled `STICKY` times its value. `cands` is
/// `(market, modelled usd/day, quoted now)`.
pub(crate) fn select(cands: &[(String, f64, bool)], min: f64, max: usize) -> BTreeSet<String> {
    let mut ranked: Vec<(f64, &str)> = cands
        .iter()
        .filter(|(_, v, now)| if *now { *v >= min / STICKY } else { *v >= min })
        .filter(|(_, v, _)| *v > 0.0)
        .map(|(m, v, now)| (if *now { v * STICKY } else { *v }, m.as_str()))
        .collect();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(b.1)));
    ranked.into_iter().take(max).map(|(_, m)| m.to_string()).collect()
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

/// Trusted Kalshi books of the configured markets: `(bids, asks)`.
static BOOKS: Mutex<BTreeMap<String, (Vec<Level>, Vec<Level>)>> = Mutex::new(BTreeMap::new());
/// The configured markets. Set once, before the feed starts.
static WATCHED: OnceLock<BTreeSet<String>> = OnceLock::new();
/// The PM-US slugs of every relationship on a configured market.
static OWNED_PM: OnceLock<Vec<String>> = OnceLock::new();
/// Market -> venue ids reported filled there since the last pass (empty: the
/// book moved).
static DIRTY: Mutex<BTreeMap<String, BTreeSet<String>>> = Mutex::new(BTreeMap::new());
/// The latest stats object, for the engine's 60 s line.
static STATS: Mutex<Option<Value>> = Mutex::new(None);

fn notify() -> &'static tokio::sync::Notify {
    static N: OnceLock<tokio::sync::Notify> = OnceLock::new();
    N.get_or_init(tokio::sync::Notify::new)
}

fn mark(market: &str, filled: Option<&str>) {
    if let Ok(mut g) = DIRTY.lock() {
        let ids = g.entry(market.to_owned()).or_default();
        if let Some(id) = filled {
            ids.insert(id.to_owned());
        }
    }
    notify().notify_one();
}

/// Claim `markets` for this module, and name the PM-US slugs whose positions
/// the reconciliation must leave alone. Called once, before the feed starts.
pub fn install(markets: BTreeSet<String>, owned_pm: Vec<String>) {
    let _ = WATCHED.set(markets);
    let _ = OWNED_PM.set(owned_pm);
}

/// Is `market` one this module quotes? False on every run without `--lip`.
pub fn watching(market: &str) -> bool {
    WATCHED.get().is_some_and(|w| w.contains(market))
}

/// The engine saw `market`'s book move. `None` means it can no longer be
/// priced from: crossed, or stamped in the future.
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

/// A fill frame named an order this module placed.
pub fn wake_fill(market: &str, venue_order_id: &str) {
    if watching(market) {
        mark(market, Some(venue_order_id));
    }
}

/// PM-US slugs the positions reconciliation must report and never complete.
pub fn owned_pm_slugs() -> Vec<String> {
    OWNED_PM.get().cloned().unwrap_or_default()
}

/// This module's gauges for the engine's stats line, once the loop has run.
pub fn stats() -> Option<Value> {
    STATS.lock().ok().and_then(|g| g.clone())
}

async fn dirty() -> BTreeMap<String, BTreeSet<String>> {
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
    /// Decide and print; never place or cancel.
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

struct Mkt {
    cfg: MarketCfg,
    until: f64,
    bid: Option<Order>,
    ask: Option<Order>,
    last_touch: [Option<Instant>; 2],
    book: Book,
    /// The venue position not yet folded into `book`, awaiting a mid to adopt it at.
    unadopted: i64,
    mid: Option<i64>,
    modelled: f64,
    /// Cancelled orders whose final fill count has not been read yet, by side
    /// (`true` = bid). Nothing new rests on the market until they are settled:
    /// a place sized off a position missing their fills could pass the cap.
    unsettled: Vec<(bool, Order)>,
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

fn append(path: &str, rec: Value) {
    if let Err(e) = crate::ledger::append(path, &rec) {
        ERRORS.fetch_add(1, Ordering::Relaxed);
        eprintln!("[lip] could not append to {path}: {e} — record was {rec}");
    }
}

fn stop_file(log_path: &str) -> String {
    format!("{log_path}.STOP")
}

pub async fn run(cfg: Cfg, w: Wiring) {
    let mut mkts: BTreeMap<String, Mkt> = BTreeMap::new();
    let records = if !std::path::Path::new(&w.log_path).exists() {
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
    // Venue truth for where each market stands. A gap with the log is
    // adopted at the first mid, so P&L counts from what is actually held.
    let venue = loop {
        let k = w.kalshi.clone();
        match blocking(move || k.net_positions().map_err(|e| e.to_string())).await {
            Ok(Ok(p)) => break p,
            Ok(Err(e)) | Err(e) => eprintln!("[lip] positions read failed ({e}); retrying in 30s"),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    };
    for m in &cfg.markets {
        let book = books.get(&m.market).copied().unwrap_or_default();
        let held = venue.get(&m.market).copied().unwrap_or(0.0).round() as i64;
        if held != book.pos {
            eprintln!(
                "[lip] {}: the venue holds {held} where {} accounts for {} — the difference \
                 is adopted at the first mid",
                m.market, w.log_path, book.pos
            );
        }
        mkts.insert(
            m.market.clone(),
            Mkt {
                cfg: m.clone(),
                until: crate::taketake::parse_iso8601_z(&m.until).unwrap_or(0.0),
                bid: None,
                ask: None,
                last_touch: [None, None],
                book,
                unadopted: held - book.pos,
                mid: None,
                modelled: 0.0,
                unsettled: Vec::new(),
            },
        );
    }
    eprintln!(
        "[lip] {}: {} market(s), {}-lots both sides at the touch, cap {} net, quoting the best {} \
         modelling >= ${:.2}/day, loss stop ${:.0}. Log {}.",
        if w.shadow { "SHADOW" } else { "ARMED" },
        mkts.len(),
        cfg.qty,
        cfg.cap,
        cfg.max_markets,
        cfg.min_usd_day,
        cfg.loss_stop_usd,
        w.log_path
    );
    let mut quoted: BTreeSet<String> = BTreeSet::new();
    let mut windows: Result<Vec<(String, f64, f64, String)>, String> = Ok(Vec::new());
    let mut windows_at: Option<Instant> = None;
    let mut wait = Duration::from_secs(1);
    loop {
        let fills = tokio::time::timeout(wait, dirty()).await.unwrap_or_default();
        if windows_at.is_none_or(|t| t.elapsed() >= BLACKOUT_REREAD) {
            windows = crate::maker_exit::read_entry_blackouts(&w.blackouts_path);
            windows_at = Some(Instant::now());
        }
        let books: BTreeMap<String, (Vec<Level>, Vec<Level>)> =
            BOOKS.lock().map(|g| g.clone()).unwrap_or_default();
        let killed = std::path::Path::new(&w.kill_file).exists();
        let stopped = std::path::Path::new(&stop_file(&w.log_path)).exists();
        let now = now_wall();

        // Settle fills first, so every decision below sees the position as it is.
        for (name, m) in mkts.iter_mut() {
            let ids = fills.get(name).cloned().unwrap_or_default();
            for side in [true, false] {
                check_fills(m, side, &ids, &w).await;
            }
            settle_cancelled(m, &w).await;
        }

        // Which markets may rest at all, and what each would model.
        let mut cands = Vec::new();
        let mut why_not: BTreeMap<String, String> = BTreeMap::new();
        for (name, m) in mkts.iter_mut() {
            let blocked = if killed {
                Some("kill file".to_string())
            } else if stopped {
                Some("loss stop".to_string())
            } else if now >= m.until {
                Some("program over".to_string())
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
            let Some((bids, asks)) = books.get(name) else {
                m.modelled = 0.0;
                why_not.insert(name.clone(), blocked.unwrap_or_else(|| "no trusted book".into()));
                continue;
            };
            let (bo, ao) = others_of(m, bids, asks, w.shadow);
            if let (Some(b), Some(a)) = (bids.first().and_then(|l| px(&l.price)), asks.first().and_then(|l| px(&l.price))) {
                m.mid = Some((b + a) / 2);
            }
            if m.unadopted != 0 {
                if let Some(mid) = m.mid {
                    let n = m.unadopted;
                    m.book.pos += n;
                    m.book.cash_e4 -= mid * n;
                    m.unadopted = 0;
                    append(
                        &w.log_path,
                        json!({"ts": now, "event": "adopt", "market": name, "qty": n, "price": px_str(mid)}),
                    );
                    eprintln!("[lip] {name}: ADOPTED {n} contract(s) the log did not account for, at mid {}", px_str(mid));
                }
            }
            let wt = want(&bo, &ao, m.book.pos, cfg.qty, cfg.cap);
            m.modelled = modelled_share(&bo, &ao, wt.bid, wt.ask) * m.cfg.usd_day;
            match blocked {
                Some(b) => {
                    why_not.insert(name.clone(), b);
                }
                None => cands.push((name.clone(), m.modelled, quoted.contains(name))),
            }
        }
        let chosen = select(&cands, cfg.min_usd_day, cfg.max_markets);
        if chosen != quoted {
            eprintln!(
                "[lip] quoting {:?} (modelled: {})",
                chosen,
                cands.iter().map(|(m, v, _)| format!("{m} ${v:.2}")).collect::<Vec<_>>().join(", ")
            );
        }
        quoted = chosen;

        // Act. Cancel before place, per side.
        let mut debounced = false;
        for (name, m) in mkts.iter_mut() {
            let wt = match (quoted.contains(name), books.get(name)) {
                (true, Some((bids, asks))) => {
                    let (bo, ao) = others_of(m, bids, asks, w.shadow);
                    want(&bo, &ao, m.book.pos, cfg.qty, cfg.cap)
                }
                _ => Want::default(),
            };
            for side in [true, false] {
                debounced |= act_side(m, side, if side { wt.bid } else { wt.ask }, &w).await;
            }
        }

        // The loss stop, over every market's inventory at its last mid.
        let pnl: f64 = mkts.values().map(|m| m.mid.map_or(0.0, |mid| m.book.pnl_at(mid))).sum();
        if pnl <= -cfg.loss_stop_usd && !stopped {
            let path = stop_file(&w.log_path);
            let why = format!("inventory P&L ${pnl:.2} reached the ${:.0} stop", cfg.loss_stop_usd);
            let _ = std::fs::write(&path, format!("{why}\nremove this file to resume\n"));
            append(&w.log_path, json!({"ts": now, "event": "stop", "pnl_usd": pnl}));
            eprintln!("[lip] ### LOSS STOP: {why}. Pulling everything; {path} holds it until removed.");
            mark(mkts.keys().next().map(String::as_str).unwrap_or(""), None);
        }

        publish(&mkts, &quoted, &why_not, pnl, stopped || pnl <= -cfg.loss_stop_usd, w.shadow);
        wait = if debounced { MIN_REQUOTE } else { HEARTBEAT.min(Duration::from_secs(30)) };
    }
}

/// Both sides of the book without our own orders in them. A shadow order never
/// reached the venue, so there is nothing of ours to take out.
fn others_of(m: &Mkt, bids: &[Level], asks: &[Level], shadow: bool) -> (Vec<(i64, f64)>, Vec<(i64, f64)>) {
    let own = |o: &Option<Order>| if shadow { None } else { o.as_ref().map(|o| (o.price, o.resting())) };
    (others(bids, own(&m.bid)), others(asks, own(&m.ask)))
}

/// Bring one side's fills up to date: on a fill frame naming the order, and
/// on the heartbeat, which also notices an order that is no longer resting.
async fn check_fills(m: &mut Mkt, bid: bool, frame_ids: &BTreeSet<String>, w: &Wiring) {
    let slot = if bid { &mut m.bid } else { &mut m.ask };
    let Some(o) = slot.as_ref() else { return };
    if w.shadow {
        return;
    }
    let named = frame_ids.contains(&o.venue_id);
    if !named && o.checked.elapsed() < HEARTBEAT {
        return;
    }
    let k = w.kalshi.clone();
    let id = o.venue_id.clone();
    // Heartbeat: is it still working? A terminal order reports its final count.
    let (filled, terminal) = if named {
        match blocking(move || k.filled_qty(&id).map_err(|e| e.to_string())).await {
            Ok(Ok(n)) => (n, false),
            Ok(Err(e)) | Err(e) => {
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("[lip] {}: fill count unreadable ({e})", m.cfg.market);
                return;
            }
        }
    } else {
        match blocking(move || k.terminal_filled_qty(&id).map_err(|e| e.to_string())).await {
            Ok(Ok(Some(n))) => (n, true),
            Ok(Ok(None)) => {
                if let Some(o) = slot.as_mut() {
                    o.checked = Instant::now();
                }
                return;
            }
            Ok(Err(e)) | Err(e) => {
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("[lip] {}: order status unreadable ({e})", m.cfg.market);
                return;
            }
        }
    };
    let market = m.cfg.market.clone();
    account(&market, &mut m.book, slot, bid, filled, w);
    if terminal {
        if let Some(o) = slot.take() {
            eprintln!(
                "[lip] {market}: {} {} @ {} is no longer resting at the venue ({} filled); re-placing",
                if bid { "bid" } else { "ask" },
                o.qty,
                px_str(o.price),
                o.filled
            );
        }
    } else if let Some(o) = slot.as_mut() {
        o.checked = Instant::now();
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
        &w.log_path,
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

/// Move one side toward `want`. Returns true when a change was held back by
/// [`MIN_REQUOTE`], so the caller wakes again soon.
async fn act_side(m: &mut Mkt, bid: bool, want: Option<(i64, i64)>, w: &Wiring) -> bool {
    let i = usize::from(!bid);
    let market = m.cfg.market.clone();
    let slot_keep = {
        let slot = if bid { &m.bid } else { &m.ask };
        slot.as_ref().map(|o| keep((o.price, o.resting()), want))
    };
    if slot_keep == Some(true) || (slot_keep.is_none() && want.is_none()) {
        return false;
    }
    if m.last_touch[i].is_some_and(|t| t.elapsed() < MIN_REQUOTE) {
        return true;
    }
    // Something must change. Cancel first: a place without the cancel that
    // precedes it could double the size resting at the venue.
    if slot_keep == Some(false) {
        m.last_touch[i] = Some(Instant::now());
        let o = (if bid { &m.bid } else { &m.ask }).clone().expect("checked above");
        if !w.shadow {
            let k = w.kalshi.clone();
            let req = CancelRequest { by: CancelBy::VenueId(o.venue_id.clone()), market_slug: None };
            match blocking(move || k.cancel(&req)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    ERRORS.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[lip] {market}: cancel of {} refused ({e}); keeping it on the books", o.venue_id);
                    return true;
                }
                Err(e) => {
                    ERRORS.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[lip] {market}: cancel of {} failed ({e}); keeping it on the books", o.venue_id);
                    return true;
                }
            }
            // A fill can race the cancel: count it before forgetting the order.
            let k = w.kalshi.clone();
            let id = o.venue_id.clone();
            let before = m.book.pos;
            match blocking(move || k.filled_qty(&id)).await {
                Ok(Ok(n)) => {
                    let slot = if bid { &mut m.bid } else { &mut m.ask };
                    account(&market, &mut m.book, slot, bid, n, w);
                    if m.book.pos != before {
                        // The position moved, so `want` was sized for one we no
                        // longer hold. The next pass decides again.
                        forget(m, bid);
                        mark(&market, None);
                        return false;
                    }
                }
                _ => {
                    ERRORS.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "[lip] {market}: could not read {}'s final fill count after cancelling; \
                         nothing rests here until it is read",
                        o.venue_id
                    );
                    m.unsettled.push((bid, o.clone()));
                    forget(m, bid);
                    return true;
                }
            }
        }
        forget(m, bid);
        // A reprice is ONE change: the replacement goes out now, not a
        // debounce later, so the side is not left empty at the touch.
    }
    let Some((p, q)) = want else { return false };
    if !m.unsettled.is_empty() {
        return true;
    }
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
        let k = w.kalshi.clone();
        let rq = req.clone();
        match blocking(move || k.place(&rq)).await {
            Ok(Ok(id)) => {
                crate::engine::fill::note_sidecar_order(&id);
                id
            }
            Ok(Err(e)) => {
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("[lip] {market}: place {} {q} @ {} refused ({e})", if bid { "bid" } else { "ask" }, px_str(p));
                return true;
            }
            Err(e) => {
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("[lip] {market}: place task failed ({e})");
                return true;
            }
        }
    };
    PLACED.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[lip] {}REST {market} {} {q} @ {} — id {venue_id} ({coid}), position {}, modelled ${:.2}/day",
        if w.shadow { "SHADOW " } else { "" },
        if bid { "bid" } else { "ask" },
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

/// Read the final count of every cancelled order still owed one.
async fn settle_cancelled(m: &mut Mkt, w: &Wiring) {
    let mut left = Vec::new();
    for (bid, o) in std::mem::take(&mut m.unsettled) {
        let k = w.kalshi.clone();
        let id = o.venue_id.clone();
        match blocking(move || k.filled_qty(&id).map_err(|e| e.to_string())).await {
            Ok(Ok(n)) => {
                let market = m.cfg.market.clone();
                let mut slot = Some(o);
                account(&market, &mut m.book, &mut slot, bid, n, w);
            }
            _ => left.push((bid, o)),
        }
    }
    m.unsettled = left;
}

fn forget(m: &mut Mkt, bid: bool) {
    CANCELLED.fetch_add(1, Ordering::Relaxed);
    if bid {
        m.bid = None;
    } else {
        m.ask = None;
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

    #[test]
    fn selection_takes_the_best_and_holds_what_it_quotes_against_near_ties() {
        let c = |m: &str, v: f64, now: bool| (m.to_string(), v, now);
        let s = select(&[c("A", 5.0, false), c("B", 3.0, false), c("C", 1.0, false)], 2.0, 2);
        assert_eq!(s, ["A", "B"].iter().map(|x| x.to_string()).collect());
        // B is quoted; C models a little better but not STICKY times better.
        let s = select(&[c("A", 5.0, true), c("B", 3.0, true), c("C", 3.5, false)], 2.0, 2);
        assert!(s.contains("B") && !s.contains("C"));
        // A quoted market keeps going down to min/STICKY, a new one does not start.
        assert!(select(&[c("A", 1.7, true)], 2.0, 2).contains("A"));
        assert!(select(&[c("A", 1.7, false)], 2.0, 2).is_empty());
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
        let ok = "qty: 200\ncap: 200\nmax_markets: 5\nmin_usd_day: 2.0\nloss_stop_usd: 150\nmarkets:\n  - {market: KXA-1, usd_day: 14.29, until: 2026-11-15T05:00:00Z}\n";
        std::fs::write(&p, ok).unwrap();
        assert!(load(p.to_str().unwrap()).is_ok());
        for bad in [
            ok.replace("qty: 200", "qty: 300"),
            ok.replace("2026-11-15T05:00:00Z", "next week"),
            ok.replace("loss_stop_usd: 150", "loss_stop_usd: 0"),
            ok.replace("max_markets: 5", "max_markets: 0"),
            format!("{ok}  - {{market: KXA-1, usd_day: 1, until: 2026-11-15T05:00:00Z}}\n"),
            ok.replace("min_usd_day", "min_usd_per_day"),
        ] {
            std::fs::write(&p, &bad).unwrap();
            assert!(load(p.to_str().unwrap()).is_err(), "accepted:\n{bad}");
        }
    }
}
