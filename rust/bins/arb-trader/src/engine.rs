//! Single-owner engine task: books + quoters + decision policy live here and
//! nowhere else (no locks). Consumes the feed channel; emits canonical intent
//! lines (identical bytes to arb-intent / scripts/intent_replay.py) and
//! routes effect commands to per-venue executors. Time-based behavior runs on
//! deadlines (tokio intervals) in the same select loop — kill-switch watch
//! and stats — never on per-event syscalls.

use crate::exec::{Action, ExecCmd, ExecStats};
use arb_venue::gateway::{CancelRequest, PlaceRequest, Side as VenueSide, Tif};
use crate::feed::FeedMsg;
use crate::hist::Hist;
use crate::wal::Wal;
use arb_core::book::{ApplyError, BookBuilder};
use arb_core::fees::FeeSchedule;
use arb_core::fill::{dropped_unconsumed, FillLedger, HedgeAnchor};
use arb_core::model::{BookSide, Level, Venue};
use arb_core::quoter::{Quoter, RiskGate};
use arb_core::scan::{Cx, Rel};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct RunCfg {
    pub out_path: Option<String>,
    pub kill_file: String,
    pub stats_every_s: u64,
    pub bench: bool,
    /// Engine-sequenced write-ahead log (crate::wal); None = off.
    pub wal_path: Option<String>,
    /// Recorder health feed to watch; None = check disabled (bench/replay,
    /// which have no live feed and must stay byte-deterministic).
    pub health_file: Option<String>,
    /// Shared risk view. The quoters consult it per place; the engine feeds it
    /// exposure on fills. None = risk off (bench/replay).
    pub risk: Option<std::sync::Arc<crate::risk::RiskView>>,
    /// Append-only trade ledger to BOOK completed baskets into. `None` unless
    /// the order path is armed — a dry run must never write into the accounting
    /// ledger, or it would invent exposure that the next startup seeds from.
    pub ledger_path: Option<String>,
    /// Hedge-retry policy. `None` disables the deadline entirely (bench/replay,
    /// which must stay byte-deterministic).
    pub hedge_retry: Option<HedgeRetry>,
    /// Take-take policy. `None` = the detector never runs.
    pub take_take: Option<TakeTake>,
}

/// Immediately-executable crossings, tested on the book event that creates
/// them rather than on a timer (Geoff 2026-07-28: milliseconds, not minutes).
#[derive(Clone)]
pub struct TakeTake {
    /// Per-relationship concentration cap, in contracts.
    pub max_ct_per_rel: i64,
    /// Contracts per single execution.
    pub max_clip: i64,
    /// `data/exec/marks.json` — the blended-APR bar is derived from it. A
    /// missing/unreadable file falls back to `DEFAULT_BAR_APR`, never to 0.
    pub marks_path: String,
    /// Detect and log, place nothing. The shadow step before arming.
    pub detect_only: bool,
    /// Seconds a relationship is barred from re-firing after it acts.
    ///
    /// This is NOT cosmetic. A crossing persists across every book event until
    /// someone takes it, and `open_ct` — which the concentration cap reads —
    /// does not move until the fill is BOOKED. Without this gate an armed
    /// detector re-fires the same crossing on every tick in the window between
    /// placing leg 1 and booking it, i.e. hundreds of times in a second. The
    /// detect-only run on 2026-07-28 logged one fedcut crossing ~10x in
    /// seconds, which is exactly that failure with the orders removed.
    pub cooldown_s: f64,
}

#[derive(Clone)]
pub struct HedgeRetry {
    /// Seconds before an unfilled hedge is retried. Comfortably longer than a
    /// fill report takes, so a retry is not racing an outcome we already have.
    pub interval_s: f64,
    /// How far WORSE than the anchor the touch may be and still be taken. The
    /// anchor is the price at which the basket was known profitable, so this is
    /// the profit we are willing to give up to stop being naked.
    pub max_slip: String,
    /// Seconds naked before it stops being a retry and becomes an alarm.
    pub alarm_after_s: f64,
}

/// A hedge that has been placed but not (fully) filled.
struct PendingHedge {
    hedge: HedgeOrder,
    /// Event time the FIRST attempt for this maker fill went out — the age that
    /// matters is how long we have been naked, not how long since the last try.
    first_ts: f64,
    last_try_ts: f64,
    tries: u32,
    alarmed: bool,
}

/// Book a completed basket: the maker leg filled and its hedge filled, so the
/// position is real and the next restart must see it.
///
/// Deliberately NOT fee-complete. The engine knows the prices it traded at, but
/// venue fees arrive on the fill reports the reconciler reads, so writing a
/// `cost_usd` here would be a guess in the accounting record. `fees_pending`
/// says so out loud rather than shipping a confident wrong number.
#[allow(clippy::too_many_arguments)]
fn book_basket(path: &str, maker: &MakerOrder, hedge: &HedgeOrder, qty: i64, ts: f64) {
    let rec = json!({
        "ts": ts,
        "relationship_id": maker.rel_id,
        "title": format!("{} (rust maker-hedge)", maker.rel_id),
        "qty": qty,
        "strategy": "maker-hedge",
        "status": "open",
        "source": "arb-trader",
        "fees_pending": true,
        "legs": [
            {"venue": maker.venue, "market_id": maker.market_id, "side": maker.side,
             "role": "maker", "qty": qty, "yes_price": maker.price,
             "order_id": hedge.maker_order_id},
            {"venue": hedge.venue, "market_id": hedge.market_id, "side": hedge.side,
             "role": "taker", "qty": qty, "yes_price": hedge.price},
        ],
    });
    use std::io::Write as _;
    let opened = std::fs::OpenOptions::new().create(true).append(true).open(path);
    match opened {
        Ok(mut f) => {
            if let Err(e) = writeln!(f, "{rec}") {
                eprintln!("[ledger] WRITE FAILED ({e}) — basket {} is UNBOOKED", maker.rel_id);
            }
        }
        Err(e) => eprintln!("[ledger] cannot open {path}: {e} — basket UNBOOKED"),
    }
}

/// Feeds whose staleness invalidates pricing. A cross-venue quote on EITHER
/// venue is hedge-priced against the OTHER venue's book, so one stale critical
/// feed makes both sides wrong — which is why staleness pulls every quote, not
/// just the stale venue's.
const CRITICAL_FEEDS: [&str; 2] = ["kalshi-ws", "polymarket_us-ws"];

/// The recorder writes a health line per tick; the file is large, so read a
/// tail window rather than the whole thing.
fn last_line(path: &str, window: u64) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    f.seek(SeekFrom::Start(len.saturating_sub(window))).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    // The window can start mid-codepoint; lossy is fine, we only parse JSON.
    String::from_utf8_lossy(&buf).lines().last().map(|s| s.to_string())
}

/// `None` = feeds healthy; `Some(reason)` = pull the quotes.
///
/// FAIL-CLOSED, unlike the Python original (`exec/main.py` returned early and
/// left the state unchanged when the file could not be read). Python's version
/// caught the realistic failure — recorder dies, `ts` goes old — but a health
/// file that is deleted or never appears would leave it quoting forever on a
/// feed it cannot see. Refusing to quote when we cannot prove the feed is
/// healthy is the direction that cannot lose money.
fn feed_stale_reason(path: &str, now_wall: f64) -> Option<String> {
    let Some(line) = last_line(path, 4096) else {
        return Some(format!("health file {path} unreadable"));
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
        return Some(format!("health file {path} has no parseable line"));
    };
    let ts = v.get("ts").and_then(|t| t.as_f64()).unwrap_or(0.0);
    let age = now_wall - ts;
    if age > 30.0 {
        // The health WRITER is silent, which means the recorder is down — a
        // strictly worse condition than any single feed going quiet.
        return Some(format!("recorder silent for {age:.0}s"));
    }
    let stale = v.get("stale");
    let bad: Vec<&str> = CRITICAL_FEEDS
        .iter()
        .copied()
        .filter(|f| {
            stale.and_then(|s| s.get(*f)).and_then(|b| b.as_bool()).unwrap_or(false)
        })
        .collect();
    (!bad.is_empty()).then(|| format!("{} stale", bad.join(", ")))
}

/// The maker order behind a basket: everything the ledger record needs about
/// the leg we rested.
struct MakerOrder {
    rel_id: String,
    class: &'static str,
    venue: String,
    market_id: String,
    side: String,
    price: String,
}

/// May a retry take `touch`, given the anchor the basket was priced against?
///
/// The anchor is the price at which the basket was known profitable, so
/// `max_slip` is exactly how much of that edge we will surrender to stop being
/// naked. Beyond it the answer is WAIT, never chase — Geoff 2026-07-22, "hedge
/// only if profitable; otherwise find a profitable hedge in the future". The
/// naked alarm is what keeps waiting from being silent.
///
/// `book_side` is the side of the hedge leg's book we take: "bid" means we are
/// SELLING into it (worse = lower), "ask" means BUYING from it (worse = higher).
fn hedge_price_acceptable(
    cx: &mut Cx,
    book_side: &str,
    touch: &str,
    anchor: &str,
    max_slip: &str,
) -> bool {
    let a = cx.parse_exact(anchor);
    let slip = cx.parse_exact(max_slip);
    let t = cx.parse_exact(touch);
    if book_side == "bid" {
        let floor = cx.sub(a, slip);
        cx.cmp(t, floor) != std::cmp::Ordering::Less
    } else {
        let ceil = cx.add(a, slip);
        cx.cmp(t, ceil) != std::cmp::Ordering::Greater
    }
}

/// A hedge we placed, kept so its fill can be recognised and booked.
#[derive(Clone)]
struct HedgeOrder {
    /// The maker order whose fill created this hedge.
    maker_order_id: String,
    market_id: String,
    venue: &'static str,
    side: &'static str,
    price: String,
    qty: i64,
}

fn wall_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn parse_venue(s: &str) -> Option<Venue> {
    Some(match s {
        "kalshi" => Venue::Kalshi,
        "polymarket" => Venue::Polymarket,
        "polymarket_us" => Venue::PolymarketUs,
        _ => return None,
    })
}

fn levels_of(v: Option<&serde_json::Value>) -> Option<Vec<Level>> {
    let mut out = Vec::new();
    for l in v?.as_array()? {
        let price = l.get("price")?.as_str()?.to_owned();
        let size = l.get("size")?.as_str()?.to_owned();
        let p: f64 = price.parse().ok()?;
        if !(0.0..=1.0).contains(&p) {
            return None;
        }
        out.push(Level { price, size });
    }
    Some(out)
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
fn hedge_anchor(
    rel: &Rel,
    market_id: &str,
    side: &str,
    books: &BookBuilder,
    ts: f64,
) -> Option<HedgeAnchor> {
    let side: &'static str = match side {
        "bid" => "bid",
        "ask" => "ask",
        _ => return None,
    };
    let i = rel.legs.iter().position(|l| l.market_id == market_id)?;
    let hedge = rel.legs.get(1 - i)?;
    let book = books.get(hedge.venue, &hedge.market_id)?;
    let lvl = if side == "bid" { book.bids.first() } else { book.asks.first() }?;
    Some(HedgeAnchor {
        venue: hedge.venue.as_str(),
        market_id: hedge.market_id.clone(),
        side,
        price: lvl.price.clone(),
        ts,
    })
}

pub async fn run(
    mut quoters: Vec<Quoter>,
    by_market: HashMap<(Venue, String), Vec<usize>>,
    mut rx: mpsc::Receiver<FeedMsg>,
    exec_txs: HashMap<Venue, mpsc::Sender<ExecCmd>>,
    exec_stats: Arc<ExecStats>,
    cfg: RunCfg,
) -> serde_json::Value {
    let mut cx = Cx::default();
    let fees = FeeSchedule::new(&mut cx);
    let mut books = BookBuilder::new();
    let mut digest = Sha256::new();
    let decision = Hist::new();
    let (mut n_ev, mut n_book, mut n_int) = (0u64, 0u64, 0u64);
    // Crossings the detector found above the bar. In detect_only these are
    // opportunities we DECLINED to take, which is the number worth watching
    // before arming the taker path.
    let mut n_tt: u64 = 0;
    // Crossings the gate below refused to re-fire. A large number here is
    // normal and healthy — it is the same standing crossing seen again.
    let mut n_tt_gated: u64 = 0;
    let mut n_tt_fired: u64 = 0;
    let mut tt_gate = crate::taketake::Gate::default();
    let mut next_tt_oid: u64 = 0;
    // The bar is re-derived from marks on the stats tick: it moves as the book
    // turns over, and a stale bar is a wrong bar in both directions.
    let mut tt_bar: f64 = crate::taketake::DEFAULT_BAR_APR;
    if let Some(tt) = cfg.take_take.as_ref() {
        tt_bar = std::fs::read_to_string(&tt.marks_path)
            .ok()
            .and_then(|s| crate::taketake::blended_apr(&s, &crate::taketake::today_iso(wall_now())))
            .unwrap_or(crate::taketake::DEFAULT_BAR_APR);
        eprintln!(
            "[take-take] {} — bar {:.1}%/yr, cap {}ct/rel, clip {}",
            if tt.detect_only { "DETECT ONLY (places nothing)" } else { "ARMED" },
            tt_bar,
            tt.max_ct_per_rel,
            tt.max_clip
        );
    }
    let mut next_oid: u64 = 0;
    let mut intents: Vec<String> = Vec::new();
    let mut killed = false;
    // Feed-health pull (card 0a7e5478). Holds the REASON, not just a flag, so
    // a pulled engine can always say why it is silent. Starts pulled when the
    // check is on: we have not yet proven the feeds are healthy, and the first
    // tick either clears it or names the problem.
    let mut feed_reason: Option<String> =
        cfg.health_file.is_some().then(|| "startup — feeds not yet proven healthy".to_string());
    let mut last_now: f64 = 0.0;
    let mut chan_hw: usize = 0;
    let mut fills = FillLedger::new();
    // order id -> (relationship id, class). A fill arrives with our order id
    // only, but exposure is booked per relationship, so the mapping is captured
    // at place time when the rel is in hand.
    let mut order_rel: HashMap<String, MakerOrder> = HashMap::new();
    // venue's order id -> ours, learned from order_ack.
    let mut venue_oid: HashMap<String, String> = HashMap::new();
    // Hedge orders we placed, by OUR id. Deliberately separate from the
    // FillLedger: a hedge in the ledger would hedge its own fill.
    let mut hedge_orders: HashMap<String, HedgeOrder> = HashMap::new();
    let mut next_hedge_oid: u64 = 0;
    // Outstanding hedges, keyed by OUR hedge order id.
    let mut pending_hedges: HashMap<String, PendingHedge> = HashMap::new();
    // Contracts actually hedged per maker order. A retry asks only for what is
    // still missing, so a late fill arriving after a retry cannot over-hedge.
    let mut hedged_by_maker: HashMap<String, i64> = HashMap::new();
    let (mut n_retry, mut n_naked) = (0u64, 0u64);
    let (mut n_ack, mut n_fill, mut n_hedge) = (0u64, 0u64, 0u64);
    let t_start = std::time::Instant::now();
    let mut wal = cfg.wal_path.as_deref().map(Wal::spawn);

    let mut out = cfg.out_path.as_ref().map(|p| {
        if let Some(dir) = std::path::Path::new(p).parent() {
            std::fs::create_dir_all(dir).expect("out dir");
        }
        std::io::BufWriter::new(
            std::fs::OpenOptions::new().create(true).append(true).open(p).expect("out"),
        )
    });

    let mut kill_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    kill_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stats_iv =
        tokio::time::interval(std::time::Duration::from_secs(cfg.stats_every_s.max(1)));
    stats_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut feed_iv = tokio::time::interval(std::time::Duration::from_secs(5));
    feed_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut hedge_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    hedge_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // $rel: the relationship whose quoter emitted these intents (for the
    // hedge-anchor lookup at place time), or None for intents that rest
    // nothing (hedge obligations).
    macro_rules! drain_intents {
        ($rel:expr) => {
            for l in intents.drain(..) {
                digest.update(l.as_bytes());
                digest.update(b"\n");
                n_int += 1;
                if let Some(o) = out.as_mut() {
                    writeln!(o, "{l}").expect("write out");
                    if !cfg.bench {
                        o.flush().expect("flush out"); // tail -f visibility; ~80/day live
                    }
                }
                // route the effect to its venue executor (dry-run gateway seam)
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&l) {
                    // fill-ledger bookkeeping: orders enter the ledger at place
                    // time carrying their quote-time hedge anchor, so a later
                    // fill knows where to hedge without re-reading the book.
                    let ts_ev = v.get("ts").and_then(|x| x.as_f64()).unwrap_or(0.0);
                    if let (Some(mkt), Some(oid), Some(count)) = (
                        v.get("place").and_then(|x| x.as_str()),
                        v.get("order_id").and_then(|x| x.as_str()),
                        v.get("count").and_then(|x| x.as_i64()),
                    ) {
                        let side = v.get("side").and_then(|x| x.as_str()).unwrap_or("");
                        // A hedge is never registered: it has no hedge of its
                        // own, and registering it would make its fill mint one.
                        if v.get("tag").and_then(|x| x.as_str()) != Some("hedge") {
                            let anchor = $rel
                                .and_then(|r| hedge_anchor(r, mkt, side, &books, ts_ev));
                            fills.register_order(oid, mkt, count, anchor);
                        }
                        if let Some(r) = $rel {
                            order_rel.insert(
                                oid.to_string(),
                                MakerOrder {
                                    rel_id: r.id.clone(),
                                    class: r.rtype.as_str(),
                                    venue: v.get("venue").and_then(|x| x.as_str())
                                        .unwrap_or("").to_string(),
                                    market_id: mkt.to_string(),
                                    side: side.to_string(),
                                    price: v.get("price").and_then(|x| x.as_str())
                                        .unwrap_or("").to_string(),
                                },
                            );
                        }
                        // an amend retires the old id, but a fill can still
                        // race it — observe_cancel KEEPS the record.
                        if let Some(roid) = v.get("replaces").and_then(|x| x.as_str()) {
                            fills.observe_cancel(roid);
                        }
                    } else if let Some(oid) =
                        v.get("cancel").and(v.get("order_id")).and_then(|x| x.as_str())
                    {
                        fills.observe_cancel(oid);
                    }
                    // Build the REAL request from the intent. The quoter only
                    // ever rests post-only GTC makers, so tif/post_only are
                    // fixed here; a taker hedge will carry its own.
                    // `client_order_id` is our own order id, which is what makes
                    // a retried place idempotent at the venue.
                    let taker = v.get("taker").and_then(|x| x.as_bool()).unwrap_or(false);
                    let action = if let (Some(market), Some(oid)) = (
                        v.get("place").and_then(|x| x.as_str()),
                        v.get("order_id").and_then(|x| x.as_str()),
                    ) {
                        Some(Action::Place(PlaceRequest {
                            market: market.to_string(),
                            side: match v.get("side").and_then(|x| x.as_str()) {
                                Some("ask") => VenueSide::Ask,
                                _ => VenueSide::Bid,
                            },
                            price: v
                                .get("price")
                                .and_then(|x| x.as_str())
                                .unwrap_or("0")
                                .to_string(),
                            qty: v.get("count").and_then(|x| x.as_i64()).unwrap_or(0),
                            tif: if taker { Tif::Ioc } else { Tif::Gtc },
                            post_only: !taker,
                            client_order_id: oid.to_string(),
                        }))
                    } else if let (Some(market), Some(oid)) = (
                        v.get("cancel").and_then(|x| x.as_str()),
                        v.get("order_id").and_then(|x| x.as_str()),
                    ) {
                        // PM-US REQUIRES the market slug in the cancel body and
                        // we refuse to self-resolve it; the cancel intent
                        // carries it, so it rides along here. Kalshi ignores it.
                        Some(Action::Cancel(CancelRequest {
                            order_id: oid.to_string(),
                            market_slug: Some(market.to_string()),
                        }))
                    } else {
                        None
                    };
                    if let (Some(action), Some(venue)) = (
                        action,
                        v.get("venue").and_then(|x| x.as_str()).and_then(parse_venue),
                    ) {
                        if let Some(tx) = exec_txs.get(&venue) {
                            let cmd = ExecCmd {
                                t_read: std::time::Instant::now(),
                                action,
                            };
                            if tx.try_send(cmd).is_err() {
                                exec_stats
                                    .dropped
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        };
    }

    macro_rules! summary {
        () => {{
            let elapsed = t_start.elapsed().as_secs_f64();
            serde_json::json!({
                "mode": if cfg.bench { "bench" } else { "shadow" },
                "events": n_ev, "book_events": n_book, "intents": n_int,
                "take_take_found": n_tt, "take_take_bar_apr": tt_bar,
                "take_take_gated": n_tt_gated, "take_take_fired": n_tt_fired,
                "killed": killed,
                "feed_pulled": feed_reason.is_some(),
                "risk_allowed": cfg.risk.as_ref().map(|r| r.stats().0).unwrap_or(0),
                "risk_rejected": cfg.risk.as_ref().map(|r| r.stats().1).unwrap_or(0),
                "hedges_pending": pending_hedges.len(),
                "hedges_retried": n_retry,
                "hedges_naked": n_naked,
                "order_acks": n_ack, "fills": n_fill, "hedge_obligations": n_hedge,
                // programming-bug alarm: an obligation that was minted and
                // never hedged (arb_core::fill) — must stay 0.
                "dropped_unconsumed": dropped_unconsumed(),
                "would_place": exec_stats.placed.load(std::sync::atomic::Ordering::Relaxed),
                "would_cancel": exec_stats.cancelled.load(std::sync::atomic::Ordering::Relaxed),
                "exec_dropped": exec_stats.dropped.load(std::sync::atomic::Ordering::Relaxed),
                "exec_sent": exec_stats.sent.load(std::sync::atomic::Ordering::Relaxed),
                "exec_failed": exec_stats.failed.load(std::sync::atomic::Ordering::Relaxed),
                "chan_high_water": chan_hw,
                "decision_latency": decision.summary(),
                "exec_hop_latency": exec_stats.hop.summary(),
                "elapsed_s": (elapsed * 10.0).round() / 10.0,
                "eps": if elapsed > 0.0 { (n_ev as f64 / elapsed) as u64 } else { 0 },
            })
        }};
    }

    loop {
        tokio::select! {
            biased;
            msg = rx.recv() => {
                let Some(m) = msg else { break }; // feed closed (bench EOF)
                n_ev += 1;
                chan_hw = chan_hw.max(rx.len());
                // THE merge point: everything that reaches the engine passes
                // here exactly once, so this is where the WAL sequence is
                // assigned — before any parsing, so lines the engine skips are
                // still in the incident record verbatim.
                if let Some(w) = wal.as_mut() {
                    w.append(&m.line);
                }
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&m.line) else { continue };
                let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let Some(venue) = v.get("venue").and_then(|x| x.as_str()).and_then(parse_venue)
                else { continue };
                let Some(market_id) = v.get("market_id").and_then(|x| x.as_str()).map(str::to_owned)
                else { continue };
                let seq = v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0);
                let ts_local_ns = v.get("ts_local_ns").and_then(|x| x.as_i64()).unwrap_or(0);
                let ts_venue = v.get("ts_venue").and_then(|x| x.as_str()).map(str::to_owned);
                match kind {
                    "snapshot" => {
                        let (Some(bids), Some(asks)) =
                            (levels_of(v.get("bids")), levels_of(v.get("asks")))
                        else { continue };
                        books.apply_snapshot(venue, &market_id, bids, asks, seq, ts_local_ns, ts_venue);
                    }
                    "delta" => {
                        let side = match v.get("side").and_then(|x| x.as_str()) {
                            Some("bid") => BookSide::Bid,
                            Some("ask") => BookSide::Ask,
                            _ => continue,
                        };
                        let (Some(price), Some(size)) = (
                            v.get("price").and_then(|x| x.as_str()),
                            v.get("size").and_then(|x| x.as_str()),
                        ) else { continue };
                        match books.apply_delta(venue, &market_id, side, price, size, seq,
                                                ts_local_ns, ts_venue) {
                            Ok(_) => {}
                            Err(ApplyError::GapDetected { .. }) | Err(ApplyError::NotSynced) => continue,
                        }
                    }
                    // Own-order lifecycle events (P4 item 1). Schema, both
                    // kinds, on the SAME ordered channel as book events:
                    //   {"kind":"order_ack","venue":<kalshi|polymarket|
                    //    polymarket_us>,"market_id":str,"order_id":str,
                    //    "ts_local_ns":int}
                    //   {"kind":"fill","venue":...,"market_id":str,
                    //    "order_id":str,"cum":int,"ts_local_ns":int}
                    // `cum` is the venue's CUMULATIVE filled count for that
                    // order — not a delta — which is what makes the private-WS
                    // and poll paths idempotent against each other
                    // (arb_core::fill). `order_id` is ours (the id in the place
                    // intent). Unknown kinds keep being skipped.
                    "order_ack" => {
                        // The ledger already registered the order at place
                        // time (ids are ours), so an ack changes no decision
                        // state and emits no intent: digest-invisible.
                        //
                        // It carries ONE thing the engine cannot know
                        // otherwise: the venue's id for our order. Fills arrive
                        // under that id, so without this mapping a fill on a
                        // live order would match nothing and the hedge would
                        // never fire.
                        if let (Some(ours), Some(theirs)) = (
                            v.get("order_id").and_then(|x| x.as_str()),
                            v.get("venue_order_id").and_then(|x| x.as_str()),
                        ) {
                            venue_oid.insert(theirs.to_string(), ours.to_string());
                        }
                        n_ack += 1;
                        last_now = ts_local_ns as f64 / 1e9;
                        decision.record(m.t_read.elapsed().as_nanos() as u64);
                        continue;
                    }
                    "fill" => {
                        let (Some(reported), Some(cum)) = (
                            v.get("order_id").and_then(|x| x.as_str()),
                            v.get("cum").and_then(|x| x.as_i64()),
                        ) else { continue };
                        // A venue reports its own id; the ledger knows ours.
                        // Fall through to the reported id when it is already
                        // ours (the dry-run/replay case, and the poll path
                        // which looks orders up by our id).
                        let oid: &str =
                            venue_oid.get(reported).map(|s| s.as_str()).unwrap_or(reported);
                        // A hedge fill completes a basket. Hedges are not in
                        // the FillLedger (that would hedge the hedge), so they
                        // are recognised here and booked exactly once.
                        if let Some(h) = hedge_orders.remove(oid) {
                            let filled = cum.min(h.qty);
                            // Credit the MAKER order, not the hedge: a retry
                            // asks for what is still missing, so a late fill
                            // cannot cause a second hedge for the same size.
                            *hedged_by_maker.entry(h.maker_order_id.clone()).or_insert(0) += filled;
                            pending_hedges.remove(oid);
                            if let Some(mo) = order_rel.get(&h.maker_order_id) {
                                if let Some(lp) = cfg.ledger_path.as_deref() {
                                    book_basket(lp, mo, &h, filled, ts_local_ns as f64 / 1e9);
                                }
                                eprintln!(
                                    "[ledger] booked {} x{} ({} maker / {} hedge)",
                                    mo.rel_id, filled, mo.market_id, h.market_id
                                );
                            }
                            decision.record(m.t_read.elapsed().as_nanos() as u64);
                            continue;
                        }
                        n_fill += 1;
                        let now = ts_local_ns as f64 / 1e9;
                        last_now = now;
                        if let Some(ob) = fills.observe_cum_fill(oid, cum) {
                            // Book the new exposure BEFORE the hedge intent, so
                            // the next quote sees capital this fill just spent.
                            if let (Some(rv), Some(mo)) =
                                (cfg.risk.as_ref(), order_rel.get(oid))
                            {
                                rv.record_open(&mo.rel_id, mo.class, ob.qty() as f64);
                            }
                            // No anchor => no hedge target. The obligation is
                            // deliberately left unconsumed so the ledger's
                            // dropped_unconsumed() alarm surfaces it instead of
                            // an exposed leg vanishing silently.
                            if let Some(a) = ob.anchor().cloned() {
                                let (f_oid, _order_market, qty, _) = ob.into_parts();
                                n_hedge += 1;
                                intents.push(
                                    json!({"hedge_needed": a.market_id, "order_id": f_oid.clone(),
                                           "qty": qty, "anchor_price": a.price.clone(), "ts": now})
                                    .to_string(),
                                );
                                // The obligation says WHAT to hedge; this is the
                                // order that does it. Marketable IOC, not a
                                // resting quote: an unhedged leg is unbounded
                                // directional risk until resolution, so the
                                // hedge crosses rather than waits.
                                //
                                // `a.side` is the hedge-leg BOOK side we take.
                                // Taking a bid means SELLING (an ask-side
                                // order); taking an ask means BUYING.
                                let order_side = if a.side == "bid" { "ask" } else { "bid" };
                                // Price at the CURRENT touch so it actually
                                // fills. The anchor is the fallback: it was
                                // captured at place time precisely because the
                                // burst that fills you is the burst that gaps
                                // your book, so a missing level here is exactly
                                // when it is needed.
                                let px = parse_venue(a.venue)
                                    .and_then(|v| books.get(v, &a.market_id))
                                    .and_then(|b| {
                                        if a.side == "bid" {
                                            b.bids.first()
                                        } else {
                                            b.asks.first()
                                        }
                                    })
                                    .map(|l| l.price.clone())
                                    .unwrap_or_else(|| a.price.clone());
                                next_hedge_oid += 1;
                                let hoid = format!("h{next_hedge_oid}");
                                // Remembered so the fill feed can tell a hedge
                                // fill from a maker fill. Hedges are NOT in the
                                // FillLedger: registering one would let its own
                                // fill mint another hedge, forever.
                                let ho = HedgeOrder {
                                    maker_order_id: f_oid,
                                    market_id: a.market_id.clone(),
                                    venue: a.venue,
                                    side: order_side,
                                    price: px.clone(),
                                    qty,
                                };
                                if cfg.hedge_retry.is_some() {
                                    pending_hedges.insert(
                                        hoid.clone(),
                                        PendingHedge {
                                            hedge: HedgeOrder { ..ho.clone() },
                                            first_ts: now,
                                            last_try_ts: now,
                                            tries: 1,
                                            alarmed: false,
                                        },
                                    );
                                }
                                hedge_orders.insert(hoid.clone(), ho);
                                intents.push(
                                    json!({"ts": now, "place": a.market_id, "venue": a.venue,
                                           "side": order_side, "price": px, "count": qty,
                                           "order_id": hoid, "tag": "hedge", "taker": true})
                                    .to_string(),
                                );
                                drain_intents!(Option::<&Rel>::None);
                            }
                        }
                        decision.record(m.t_read.elapsed().as_nanos() as u64);
                        continue;
                    }
                    _ => continue,
                }
                n_book += 1;
                let now = ts_local_ns as f64 / 1e9;
                last_now = now;
                if !killed && feed_reason.is_none() {
                    if let Some(idxs) = by_market.get(&(venue, market_id)) {
                        for &qi in idxs {
                            quoters[qi].on_book(&mut cx, &fees, &books, now, &mut next_oid, &mut intents);
                            drain_intents!(Some(&quoters[qi].rel));
                        }
                        // Take-take on the SAME event that moved the book: the
                        // crossing exists for as long as the slower side takes
                        // to react, which is not minutes.
                        if let Some(tt) = cfg.take_take.as_ref() {
                            let today = crate::taketake::today_iso(now);
                            for &qi in idxs {
                                let open = cfg
                                    .risk
                                    .as_ref()
                                    .map(|r| r.open_ct(&quoters[qi].rel.id))
                                    .unwrap_or(0.0) as i64;
                                let found = crate::taketake::detect(
                                    &mut cx,
                                    &quoters[qi].rel,
                                    &books,
                                    &today,
                                    tt_bar,
                                    tt.max_ct_per_rel,
                                    open,
                                    tt.max_clip,
                                );
                                let Ok(c) = found else { continue };
                                n_tt += 1;
                                // The SAME crossing is present on every event
                                // until someone takes it, and exposure does not
                                // move until a fill books. Without this the
                                // armed path would re-place it every tick.
                                if !tt_gate.take(&c.rel_id, now, tt.cooldown_s) {
                                    n_tt_gated += 1;
                                    continue;
                                }
                                if tt.detect_only {
                                    eprintln!(
                                        "[take-take] FOUND {} x{} edge={} net={} apr={:.0}%/yr \
                                         (bar {:.0}%) — buy {} @{} / sell {} @{} \
                                         [DETECT ONLY — nothing sent]",
                                        c.rel_id, c.size, c.edge, c.net, c.apr, tt_bar,
                                        c.kalshi_market, c.kalshi_ask, c.pmus_market, c.pmus_bid,
                                    );
                                    continue;
                                }
                                // Capital caps, balances and topic budgets are
                                // the maker path's gate too — take-take is a
                                // different reason to trade, not a licence to
                                // ignore how much is already committed.
                                //
                                // The risk gate's `opportunity_apr` overflow
                                // (risk.rs:208) is deliberately NOT supplied:
                                // it would let a great crossing exceed normal
                                // caps, and not taking that allowance is the
                                // conservative direction.
                                if let Some(rv) = cfg.risk.as_ref() {
                                    let v = rv.check(
                                        &quoters[qi].rel,
                                        Venue::PolymarketUs,
                                        c.size,
                                    );
                                    if !v.allowed {
                                        eprintln!(
                                            "[take-take] REFUSED {} x{} apr={:.0}%/yr — {}",
                                            c.rel_id, c.size, c.apr, v.reasons.join("; ")
                                        );
                                        continue;
                                    }
                                }
                                // Leg 1 ONLY. It is the constrained leg, and
                                // its fill mints the Kalshi hedge through the
                                // same anchor path a maker fill uses — so leg 2
                                // inherits retry, escalation, the naked alarm
                                // and ledger booking rather than duplicating
                                // them. `taker` makes it a marketable IOC.
                                next_tt_oid += 1;
                                n_tt_fired += 1;
                                eprintln!(
                                    "[take-take] FIRE {} x{} edge={} net={} apr={:.0}%/yr \
                                     (bar {:.0}%) — sell {} @{} then buy {} @{}",
                                    c.rel_id, c.size, c.edge, c.net, c.apr, tt_bar,
                                    c.pmus_market, c.pmus_bid, c.kalshi_market, c.kalshi_ask,
                                );
                                intents.push(
                                    json!({"place": c.pmus_market,
                                           "order_id": format!("t{next_tt_oid}"),
                                           "count": c.size, "side": "ask",
                                           "price": c.pmus_bid, "venue": "polymarket_us",
                                           "tag": "take-take", "taker": true, "ts": now})
                                    .to_string(),
                                );
                                drain_intents!(Some(&quoters[qi].rel));
                            }
                        }
                    }
                }
                decision.record(m.t_read.elapsed().as_nanos() as u64);
            }
            _ = kill_iv.tick() => {
                let kill_now = std::path::Path::new(&cfg.kill_file).exists();
                if kill_now && !killed {
                    killed = true;
                    eprintln!("[engine] KILL switch on ({}) — cancelling all resting quotes", cfg.kill_file);
                    for q in quoters.iter_mut() {
                        q.cancel_all(&mut cx, last_now, &mut intents);
                        drain_intents!(Some(&q.rel));
                    }
                } else if !kill_now && killed {
                    killed = false;
                    eprintln!("[engine] KILL switch cleared — quoting resumes");
                }
            }
            _ = hedge_iv.tick(), if cfg.hedge_retry.is_some() && !cfg.bench => {
                let pol = cfg.hedge_retry.as_ref().expect("guarded above");
                let mut retries: Vec<(String, PendingHedge)> = Vec::new();
                for (hoid, p) in pending_hedges.iter_mut() {
                    if last_now - p.last_try_ts < pol.interval_s {
                        continue;
                    }
                    // How much of this maker fill is still unhedged. A hedge
                    // that filled late is already credited here, so a retry
                    // never re-buys what we already have.
                    let done = hedged_by_maker.get(&p.hedge.maker_order_id).copied().unwrap_or(0);
                    let missing = p.hedge.qty - done;
                    if missing <= 0 {
                        retries.push((hoid.clone(), PendingHedge { tries: 0, ..PendingHedge {
                            hedge: p.hedge.clone(), first_ts: p.first_ts,
                            last_try_ts: p.last_try_ts, tries: 0, alarmed: p.alarmed } }));
                        continue; // fully hedged after all — retire below
                    }
                    let naked_for = last_now - p.first_ts;
                    if naked_for >= pol.alarm_after_s && !p.alarmed {
                        p.alarmed = true;
                        n_naked += 1;
                        eprintln!(
                            "[hedge] NAKED {}x {} on {} for {naked_for:.0}s after {} tries — \
                             the book has not offered a price that keeps the basket profitable",
                            missing, p.hedge.market_id, p.hedge.venue, p.tries
                        );
                    }
                    // Retry at the CURRENT touch, but never worse than the
                    // anchor by more than max_slip. Geoff 2026-07-22: "hedge
                    // only if profitable; otherwise find a profitable hedge in
                    // the future" — so an unprofitable book is a WAIT, never a
                    // chase. The naked alarm above is what stops waiting from
                    // being silent.
                    let book_side = if p.hedge.side == "ask" { "bid" } else { "ask" };
                    let touch = parse_venue(p.hedge.venue)
                        .and_then(|v| books.get(v, &p.hedge.market_id))
                        .and_then(|b| {
                            if book_side == "bid" { b.bids.first() } else { b.asks.first() }
                        })
                        .map(|l| l.price.clone());
                    let Some(touch) = touch else { continue };
                    if !hedge_price_acceptable(
                        &mut cx, book_side, &touch, &p.hedge.price, &pol.max_slip,
                    ) {
                        p.last_try_ts = last_now;
                        continue; // wait for a better book
                    }
                    let mut h = p.hedge.clone();
                    h.qty = missing;
                    h.price = touch;
                    retries.push((
                        hoid.clone(),
                        PendingHedge {
                            hedge: h,
                            first_ts: p.first_ts,
                            last_try_ts: last_now,
                            tries: p.tries + 1,
                            alarmed: p.alarmed,
                        },
                    ));
                }
                for (old_hoid, mut p) in retries {
                    pending_hedges.remove(&old_hoid);
                    if p.tries == 0 {
                        continue; // fully hedged; nothing to re-place
                    }
                    next_hedge_oid += 1;
                    let hoid = format!("h{next_hedge_oid}");
                    n_retry += 1;
                    eprintln!(
                        "[hedge] retry {} {}x {} @ {} (try {})",
                        hoid, p.hedge.qty, p.hedge.market_id, p.hedge.price, p.tries
                    );
                    intents.push(
                        json!({"ts": last_now, "place": p.hedge.market_id,
                               "venue": p.hedge.venue, "side": p.hedge.side,
                               "price": p.hedge.price, "count": p.hedge.qty,
                               "order_id": hoid, "tag": "hedge", "taker": true,
                               "retry": p.tries})
                        .to_string(),
                    );
                    // The OLD hedge id stays in hedge_orders so a late fill on
                    // it still books; only the pending entry moves.
                    p.last_try_ts = last_now;
                    hedge_orders.insert(hoid.clone(), p.hedge.clone());
                    pending_hedges.insert(hoid, p);
                    drain_intents!(Option::<&Rel>::None);
                }
            }
            _ = feed_iv.tick(), if cfg.health_file.is_some() => {
                let path = cfg.health_file.as_deref().expect("guarded above");
                let was_stale = feed_reason.is_some();
                let now_reason = feed_stale_reason(path, wall_now());
                // Log on any CHANGE of reason, not just healthy<->stale: an
                // engine that is silent must always be able to say why, and
                // "unreadable path" vs "recorder silent" are different bugs.
                if now_reason != feed_reason {
                    match &now_reason {
                        Some(why) => {
                            eprintln!("[engine] FEED STALE ({why}) — quotes pulled");
                            // Only sweep on the way IN; a stale->stale reason
                            // change has nothing resting left to cancel.
                            if !was_stale {
                                for q in quoters.iter_mut() {
                                    q.cancel_all(&mut cx, last_now, &mut intents);
                                    drain_intents!(Some(&q.rel));
                                }
                            }
                        }
                        None => eprintln!("[engine] feeds healthy — quoting resumes"),
                    }
                    feed_reason = now_reason;
                }
            }
            _ = stats_iv.tick(), if !cfg.bench => {
                println!("{}", summary!());
                if let Some(o) = out.as_mut() { o.flush().expect("flush"); }
                // Re-derive the bar: marks are rewritten by arbbot-marks.timer
                // as the book turns over, and holding the startup value would
                // let the engine trade against a stale definition of "good".
                if let Some(tt) = cfg.take_take.as_ref() {
                    tt_bar = std::fs::read_to_string(&tt.marks_path)
                        .ok()
                        .and_then(|s| crate::taketake::blended_apr(
                            &s, &crate::taketake::today_iso(wall_now())))
                        .unwrap_or(crate::taketake::DEFAULT_BAR_APR);
                }
            }
        }
    }

    if let Some(o) = out.as_mut() {
        o.flush().expect("final flush");
    }
    let mut s = summary!();
    if cfg.bench {
        s["sha256"] = serde_json::json!(format!("{:x}", digest.finalize()));
    }
    s
}

#[cfg(test)]
mod take_take_wiring_tests {
    use super::*;
    use arb_core::scan::{RelLeg, RelType};

    /// The whole take-take execution design rests on ONE assumption: that a
    /// leg-1 sell on PM-US derives a hedge anchor pointing at the Kalshi leg's
    /// ASK — i.e. leg 2 BUYS Kalshi, completing the K->PM basket.
    ///
    /// `detect_only` is forced on whenever the order path is unarmed, so the
    /// fire path cannot be exercised end-to-end without real money. This pins
    /// the assumption directly instead.
    #[test]
    fn leg1_sell_on_pmus_anchors_a_kalshi_buy() {
        let rel = Rel {
            id: "xvus-nobel-peace-26-elonmusk".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        };
        let mut books = BookBuilder::new();
        books.apply_snapshot(
            Venue::Kalshi,
            "K",
            vec![Level { price: "0.03".into(), size: "50".into() }],
            vec![Level { price: "0.04".into(), size: "50".into() }],
            1,
            0,
            None,
        );
        books.apply_snapshot(
            Venue::PolymarketUs,
            "P",
            vec![Level { price: "0.08".into(), size: "50".into() }],
            vec![Level { price: "0.09".into(), size: "50".into() }],
            1,
            0,
            None,
        );
        // leg 1 is an ASK-side order on the PM-US market (we sell PM YES)
        let a = hedge_anchor(&rel, "P", "ask", &books, 1.0).expect("anchor on the other leg");
        assert_eq!(a.venue, "kalshi", "hedge must be the OTHER leg");
        assert_eq!(a.market_id, "K");
        assert_eq!(a.side, "ask", "we take Kalshi's ask, i.e. we BUY");
        assert_eq!(a.price, "0.04", "at the Kalshi ask the crossing was priced against");
        // and the engine turns an 'ask' anchor into a bid-side (buy) order
        let order_side = if a.side == "bid" { "ask" } else { "bid" };
        assert_eq!(order_side, "bid", "leg 2 must BUY Kalshi");
    }
}

#[cfg(test)]
mod feed_health_tests {
    use super::*;
    use std::io::Write;

    fn health_file(lines: &[&str]) -> (tempdir::Dir, String) {
        let d = tempdir::Dir::new();
        let p = d.path().join("health.jsonl");
        let mut f = std::fs::File::create(&p).unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        let s = p.to_string_lossy().to_string();
        (d, s)
    }

    const NOW: f64 = 1_000_000.0;

    fn line(ts: f64, kalshi: bool, pmus: bool) -> String {
        format!(
            r#"{{"ts":{ts},"stale":{{"kalshi-ws":{kalshi},"polymarket_us-ws":{pmus},"polymarket-ws":true}}}}"#
        )
    }

    #[test]
    fn healthy_feeds_do_not_pull() {
        let (_d, p) = health_file(&[&line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW), None);
    }

    /// polymarket (INTL) is not a critical feed — the money path is Kalshi and
    /// PM-US, so intl staleness must not pull quotes. The fixture always sets
    /// polymarket-ws stale.
    #[test]
    fn a_non_critical_feed_going_stale_does_not_pull() {
        let (_d, p) = health_file(&[&line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW), None);
    }

    #[test]
    fn either_critical_feed_pulls_all_quotes() {
        for (k, pm, want) in [(true, false, "kalshi-ws"), (false, true, "polymarket_us-ws")] {
            let (_d, p) = health_file(&[&line(NOW - 1.0, k, pm)]);
            let why = feed_stale_reason(&p, NOW).expect("must pull");
            assert!(why.contains(want), "{why}");
        }
    }

    /// The health writer going quiet means the recorder is down — worse than
    /// any single feed, and the flags in the last line are stale evidence.
    #[test]
    fn a_silent_recorder_is_stale_even_when_the_last_line_looked_healthy() {
        let (_d, p) = health_file(&[&line(NOW - 120.0, false, false)]);
        let why = feed_stale_reason(&p, NOW).expect("must pull");
        assert!(why.contains("recorder silent"), "{why}");
    }

    /// Only the LAST line counts; an old healthy line must not rescue a new
    /// stale one.
    #[test]
    fn only_the_last_line_is_read() {
        let (_d, p) = health_file(&[&line(NOW - 5.0, false, false), &line(NOW - 1.0, true, false)]);
        assert!(feed_stale_reason(&p, NOW).is_some(), "the newest line is stale");
    }

    /// FAIL-CLOSED: no file, no readable line, or garbage all pull the quotes.
    /// Python left the state unchanged here, which would quote forever on a
    /// feed it could not see.
    #[test]
    fn an_unreadable_health_file_pulls_quotes() {
        assert!(feed_stale_reason("/nonexistent/health.jsonl", NOW).is_some());
        let (_d, p) = health_file(&["not json at all"]);
        assert!(feed_stale_reason(&p, NOW).is_some());
        let (_d2, p2) = health_file(&[]);
        assert!(feed_stale_reason(&p2, NOW).is_some(), "an empty file proves nothing");
    }

    /// A line with no `ts` is treated as infinitely old, not as ts=now.
    #[test]
    fn a_line_without_a_timestamp_is_stale() {
        let (_d, p) = health_file(&[r#"{"stale":{"kalshi-ws":false,"polymarket_us-ws":false}}"#]);
        assert!(feed_stale_reason(&p, NOW).is_some());
    }

    /// The tail window can start mid-codepoint; that must not panic or hide a
    /// healthy line.
    #[test]
    fn a_large_file_reads_only_its_tail() {
        let pad = format!(r#"{{"ts":1,"note":"{}"}}"#, "é".repeat(3000));
        let (_d, p) = health_file(&[&pad, &line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW), None);
    }
}

/// Minimal scratch dir for tests (no dev-dependency needed).
#[cfg(test)]
mod tempdir {
    pub struct Dir(std::path::PathBuf);
    impl Dir {
        pub fn new() -> Dir {
            let base = std::env::temp_dir().join(format!(
                "arb-trader-test-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&base);
            std::fs::create_dir_all(&base).unwrap();
            Dir(base)
        }
        pub fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod ledger_write_tests {
    use super::*;

    /// The round trip that matters: a basket this engine books must be read
    /// back as OPEN exposure by the same seeding path used at startup. If these
    /// two disagree, an armed engine forgets its own positions on restart.
    #[test]
    fn a_booked_basket_seeds_exposure_on_the_next_startup() {
        let dir = std::env::temp_dir().join(format!("arb-book-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.jsonl");
        let p = path.to_str().unwrap();

        let maker = MakerOrder {
            rel_id: "xvus-demo".into(),
            class: "cross-venue-equivalent",
            venue: "kalshi".into(),
            market_id: "KXDEMO".into(),
            side: "bid".into(),
            price: "0.31".into(),
        };
        let hedge = HedgeOrder {
            maker_order_id: "m1".into(),
            market_id: "demo-slug".into(),
            venue: "polymarket_us",
            side: "ask",
            price: "0.40".into(),
            qty: 5,
        };
        book_basket(p, &maker, &hedge, 5, 1_700_000_000.0);
        book_basket(p, &maker, &hedge, 3, 1_700_000_100.0);

        let recs = crate::ledger::read(p).unwrap();
        let open = crate::ledger::open_exposure(recs);
        assert_eq!(
            open.get("xvus-demo"),
            Some(&8.0),
            "both baskets must read back as open exposure"
        );

        let first: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(p).unwrap().lines().next().unwrap())
                .unwrap();
        assert_eq!(first["status"], "open");
        assert_eq!(first["qty"], 5);
        assert_eq!(first["legs"][0]["venue"], "kalshi");
        assert_eq!(first["legs"][0]["role"], "maker");
        assert_eq!(first["legs"][1]["venue"], "polymarket_us");
        assert_eq!(first["legs"][1]["role"], "taker");
        assert_eq!(
            first["fees_pending"], true,
            "the engine does not know venue fees; the record must not pretend otherwise"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod hedge_retry_tests {
    use super::*;

    fn ok(book_side: &str, touch: &str, anchor: &str, slip: &str) -> bool {
        let mut cx = Cx::default();
        hedge_price_acceptable(&mut cx, book_side, touch, anchor, slip)
    }

    /// Selling into a bid: a HIGHER bid is better than we expected, always fine.
    #[test]
    fn selling_takes_any_price_at_or_above_the_anchor() {
        assert!(ok("bid", "0.40", "0.40", "0.00"), "exactly the anchor");
        assert!(ok("bid", "0.45", "0.40", "0.00"), "better than the anchor");
    }

    /// ...and gives up at most max_slip of the edge below it.
    #[test]
    fn selling_gives_up_at_most_max_slip() {
        assert!(ok("bid", "0.39", "0.40", "0.01"), "exactly at the tolerance");
        assert!(!ok("bid", "0.38", "0.40", "0.01"), "past it => WAIT, never chase");
    }

    /// Buying from an ask: worse means paying MORE.
    #[test]
    fn buying_gives_up_at_most_max_slip() {
        assert!(ok("ask", "0.40", "0.40", "0.00"));
        assert!(ok("ask", "0.35", "0.40", "0.00"), "cheaper than expected is fine");
        assert!(ok("ask", "0.41", "0.40", "0.01"));
        assert!(!ok("ask", "0.42", "0.40", "0.01"));
    }

    /// Zero tolerance means the anchor is a hard floor/ceiling — the setting
    /// that never gives up a cent of the basket's edge.
    #[test]
    fn zero_slip_refuses_any_worse_price() {
        assert!(!ok("bid", "0.3999", "0.40", "0"));
        assert!(!ok("ask", "0.4001", "0.40", "0"));
    }

    /// The direction must not be symmetric — swapping the side must flip which
    /// way is "worse", or a retry would chase in one direction.
    #[test]
    fn the_two_sides_are_not_symmetric() {
        assert!(ok("bid", "0.50", "0.40", "0.00"), "selling higher is better");
        assert!(!ok("ask", "0.50", "0.40", "0.00"), "buying higher is worse");
    }
}
