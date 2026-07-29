//! Single-owner engine task: books + quoters + decision policy live here and
//! nowhere else (no locks). Consumes the feed channel; emits canonical intent
//! lines (identical bytes to arb-intent / scripts/intent_replay.py) and
//! routes effect commands to per-venue executors. Time-based behavior runs on
//! deadlines (tokio intervals) in the same select loop — kill-switch watch
//! and stats — never on per-event syscalls.

use crate::exec::{Action, ExecCmd, ExecStats};
use arb_venue::gateway::{CancelBy, CancelRequest, PlaceRequest, Side as VenueSide, Tif};
use crate::feed::FeedMsg;
use crate::hist::Hist;
use crate::wal::Wal;
use arb_core::book::{ApplyError, BookBuilder};
use arb_core::clock::now_s as wall_now;
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
    /// Whether the venue order path is live. Reported in the stats `mode`.
    ///
    /// This existed only as an inference before (`ledger_path.is_some()`), so
    /// every armed run still reported `mode: "shadow"` — the dashboard and any
    /// log-based monitor would say NOT TRADING while it placed real orders.
    pub armed: bool,
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
    /// missing/unreadable file falls back to `DEFAULT_BAR_APR`, never to 0; a
    /// file that is PRESENT but stale or corrupt yields no bar at all and
    /// take-take does not run (`taketake::Bar`).
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
struct PendingHedge {
    /// The maker order whose fill created this obligation (ledger attribution).
    maker_order_id: String,
    /// Contracts owed. Set once from the maker fill delta; NEVER recomputed
    /// from an attempt's size, which is where the double-subtraction came from.
    owed: i64,
    /// Contracts hedged so far, over every attempt in the chain.
    filled: i64,
    /// Where the hedge leg stood when the basket was proven profitable. Held
    /// for the LIFE of the obligation: it is the only price `max_slip` may be
    /// measured against, and it also carries the hedge leg's venue/market/side.
    anchor: HedgeAnchor,
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
    first_at: std::time::Instant,
    /// MONOTONIC time the last attempt was PLACED — same reasoning. `interval_s`
    /// gates placements, so this only moves on a real place.
    last_try_at: std::time::Instant,
    /// Our id for the most recent attempt, or `None` if none was ever placed
    /// (the first attempt can be refused by the slip gate). Read to decide
    /// whether this obligation has anything at the venue whose `order_ack` could
    /// still be in flight.
    latest_attempt: Option<String>,
    tries: u32,
    alarmed: bool,
    /// Whether the ack-hold has already been logged for this obligation. One
    /// line per obligation: the decision is re-taken every second.
    hold_logged: bool,
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
        "title": format!("{} (rust {})", maker.rel_id, maker.strategy),
        "qty": qty,
        "strategy": maker.strategy,
        "status": "open",
        "source": "arb-trader",
        "fees_pending": true,
        "legs": [
            {"venue": maker.venue, "market_id": maker.market_id, "side": maker.side,
             "role": maker.role(), "qty": qty, "yes_price": maker.price,
             "order_id": hedge.maker_order_id},
            {"venue": hedge.venue, "market_id": hedge.market_id, "side": hedge.side,
             "role": "taker", "qty": qty, "yes_price": hedge.price},
        ],
    });
    if let Err(e) = crate::ledger::append(path, &rec) {
        // The priority used to be inverted: a WAL write failure crash-stops the
        // process (wal.rs), while a LEDGER write failure was one stderr line and
        // trading carried on. The ledger is the more important file — an
        // unbooked basket is exposure no restart can see, so the caps free
        // themselves against it and nothing ever unwinds it — so it now stops
        // trading on the same terms, through the same clean halt: the book is
        // cancelled and PROVEN empty before the exit.
        eprintln!("[ledger] WRITE FAILED ({e}) — basket {} is UNBOOKED", maker.rel_id);
        eprintln!("[ledger] RECOVER BY HAND — append this line to {path}: {rec}");
        crate::exec::spawn_halt_and_exit(70, format!("ledger write failed: {e}"));
    }
}

/// Venues whose feed this engine cannot turn into an order, so their staleness
/// is not a reason to pull the money path's quotes.
///
/// Only `polymarket` (INTL), and it is STRUCTURAL rather than configuration:
/// `main::build_sinks` can construct a Kalshi sink and a PM-US sink and nothing
/// else, because PM INTL order placement is geoblocked from this host and the
/// feed is carried for data only.
///
/// KNOWN RESIDUAL, recorded rather than papered over: 6 of the 40 relationships
/// quoting on 2026-07-28 are Kalshi<->INTL (`xv-dem-nom-2028-*`,
/// `xv-rep-nom-2028-*`, human-vetted), and their KALSHI maker quote is
/// hedge-priced off the INTL book — so a stale INTL feed really can mis-price an
/// order we are able to place. It is excluded anyway because the pull is GLOBAL:
/// making INTL critical would silence all 40 relationships every time a
/// data-only feed hiccups, and INTL accumulated 1,444 s of staleness on
/// 2026-07-28 alone. The real fix is a per-relationship pull (pull the quoters
/// whose legs touch the stale venue, not every quoter), which is a change to the
/// quote decision path and not a feed-health change. Those 6 also have a worse
/// problem first: their hedge leg has no order path at all, so a fill on the
/// Kalshi leg is naked by construction.
const DATA_ONLY_VENUES: [Venue; 1] = [Venue::Polymarket];

/// The health-file staleness keys the engine requires EVIDENCE for: one per
/// venue it quotes and can place on, named `"{venue}-ws"` as the recorder names
/// them.
///
/// A cross-venue quote on EITHER venue is hedge-priced against the OTHER
/// venue's book, so one stale feed makes both sides wrong — which is why
/// staleness pulls every quote, not just the stale venue's, and why BOTH legs'
/// venues are required.
///
/// Derived rather than the literal `["kalshi-ws", "polymarket_us-ws"]` it used
/// to be, because the absent-key rule in `feed_stale_reason` only fails closed
/// if the required set tracks what we actually trade. A literal could neither
/// add a venue the registry started quoting nor drop one it stopped — and paired
/// with the old absent-reads-as-healthy rule, a recorder that RENAMED a feed
/// silently disabled the check for it. Not hypothetical: `data/health.jsonl`
/// carried `kalshi-rest` and no `kalshi-ws` at all for 37,639 lines on
/// 2026-07-20, and every one of them read as Kalshi-healthy on no evidence.
///
/// Deriving it is also what keeps the absent-key rule from wedging: a venue this
/// registry does not quote may be missing from the health file forever without
/// pulling a single quote, and a NEW tradable venue is required the moment the
/// registry quotes it rather than whenever someone remembers to add it here.
fn required_feeds(by_market: &HashMap<(Venue, String), Vec<usize>>) -> Vec<String> {
    let mut v: Vec<String> = by_market
        .keys()
        .map(|(venue, _)| *venue)
        .filter(|venue| !DATA_ONLY_VENUES.contains(venue))
        .map(|venue| format!("{}-ws", venue.as_str()))
        .collect();
    v.sort();
    v.dedup();
    v
}

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
///
/// `required` is `required_feeds()` — the venues we quote and can place on. An
/// ABSENT key in `stale` reads as STALE, the other half of failing closed: until
/// 2026-07-28 an absent key read as healthy (`unwrap_or(false)`), so the one
/// venue the recorder happened not to report was the one venue the engine could
/// never pull for.
fn feed_stale_reason(path: &str, now_wall: f64, required: &[String]) -> Option<String> {
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
    let bad: Vec<String> = required
        .iter()
        .filter_map(|f| match stale.and_then(|s| s.get(f)).and_then(|b| b.as_bool()) {
            Some(true) => Some(format!("{f} stale")),
            Some(false) => None,
            // No entry is not a healthy entry. The recorder reporting nothing
            // about a venue we trade is indistinguishable, from here, from that
            // venue's socket being half-open — so it reads the same way.
            None => Some(format!("{f} unreported by the recorder")),
        })
        .collect();
    (!bad.is_empty()).then(|| bad.join(", "))
}

/// How long after a reconnect the engine holds its quotes while the welcome
/// snapshot burst lands.
///
/// The recorder answers a new subscriber with a snapshot for EVERY market it
/// holds a book for, synchronously and before any further delta
/// (`arb_tape::broadcast`, `RecorderCore.snapshot_events`) — ~1.4 MB / a few
/// thousand lines today. The engine consumes that at >100k events/s
/// (docs/bench-recorder-baseline.md), i.e. tens of milliseconds, so two seconds
/// is a ~50x margin. Cost: the pull clears on the first 5s health tick at which
/// this has elapsed, so a reconnect gives up 5-10s of quoting — measured at 5.3s
/// against the live socket on 2026-07-28, against ~10 outages that day.
///
/// Bounded on purpose. "Every market we quote must be re-snapshotted" is the
/// stronger rule and it can WEDGE: the recorder drops closed and resolved
/// markets from its universe every 30 minutes, so one resolved leg would hold
/// every other quote hostage forever.
const RESYNC_SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

/// The engine's view of its OWN subscription to the recorder — a different fact
/// from what the recorder says about the venues' sockets, and until 2026-07-28
/// the engine had no view of it at all. See `crate::feed::FEED_DOWN`.
#[derive(Debug, PartialEq, Eq)]
enum Link {
    /// No subscription. Books are frozen wherever the drop left them.
    Down,
    /// Re-subscribed at `since`, with `snapshots` welcome snapshots applied
    /// since. NOT yet current — a reconnect is when the books become
    /// repairABLE, not when they are repaired.
    Resyncing { since: std::time::Instant, snapshots: u64 },
    /// The welcome burst landed and was consumed. Bench/replay start here: the
    /// tape IS the feed and it cannot disconnect.
    Fresh,
}

/// Why the engine's own subscription is not yet evidence that its books are
/// current. `None` = it is.
fn resync_reason(link: &Link, now: std::time::Instant) -> Option<String> {
    match link {
        Link::Fresh => None,
        Link::Down => Some("feed disconnected — books frozen where the drop left them".into()),
        // Two INDEPENDENT pieces of evidence are required before a reconnect
        // counts: that the welcome burst started (a snapshot really arrived, so
        // a listener that accepts and then says nothing keeps quotes pulled),
        // and that it has had time to finish.
        Link::Resyncing { snapshots: 0, .. } => {
            Some("feed reconnected — no welcome snapshot has arrived yet".into())
        }
        Link::Resyncing { since, .. } => {
            let waited = now.saturating_duration_since(*since);
            (waited < RESYNC_SETTLE).then(|| {
                format!(
                    "feed reconnected {:.1}s ago — welcome snapshot burst still settling",
                    waited.as_secs_f64()
                )
            })
        }
    }
}

/// What one feed-health tick should do.
#[derive(Debug, PartialEq, Eq)]
struct FeedTick {
    /// The reason to stay pulled, or `None` to quote.
    reason: Option<String>,
    /// The reason CHANGED — say it out loud.
    log: bool,
    /// The engine has just gone from quoting to pulled: cancel every quote and
    /// PROVE the venue books empty.
    sweep: bool,
    /// The subscription has proven itself; stop re-deriving that.
    proven: bool,
}

/// The feed gate for one tick. `link_reason` is `resync_reason`, `health_reason`
/// is `feed_stale_reason` (`None` when there is no health file to read).
///
/// Extracted because the two rules that matter here were untestable inside a
/// `tokio::select!` over live channels, and one of them was simply missing: the
/// pull cancelled the quotes the engine still held ids for and swept nothing, so
/// `feed_pulled: true` could sit over real orders resting on a book the engine
/// could no longer see (C4(c)).
fn feed_tick(
    was: Option<&String>,
    link_reason: Option<String>,
    health_reason: Option<String>,
) -> FeedTick {
    // Our own subscription outranks the recorder's report: if WE are not
    // connected, a health file that still looks fine says nothing about our
    // books.
    let proven = link_reason.is_none();
    let reason = link_reason.or(health_reason);
    let log = reason.as_deref() != was.map(String::as_str);
    // Only on the way IN. A pulled->pulled reason change has nothing resting
    // left to cancel, and a sweep per tick would burn the rate budget the order
    // path needs.
    let sweep = reason.is_some() && was.is_none();
    FeedTick { reason, log, sweep, proven }
}

/// The take-take bar as the marks file currently supports it.
///
/// An unreadable file is "no file" — a cold start, which `bar_from_marks`
/// answers with `DEFAULT_BAR_APR`. A file that is PRESENT and stale or corrupt
/// is a refusal, and the two must not share an answer.
fn read_bar(path: &str) -> crate::taketake::Bar {
    crate::taketake::bar_from_marks(&std::fs::read_to_string(path).unwrap_or_default(), wall_now())
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
    /// Which strategy opened this leg — `maker-hedge` or `take-take`.
    ///
    /// Take-take reuses the maker fill -> hedge pipeline, which is the right
    /// call mechanically but made the ACCOUNTING lie: every take-take basket
    /// booked as `strategy: maker-hedge` with its leg 1 tagged `role: maker`,
    /// when leg 1 was a marketable IOC. P&L could not be attributed between
    /// the two strategies, and Python's auto_take_take.py writes `take-take`,
    /// so the same trade had two names depending on which stack made it.
    strategy: &'static str,
}

impl MakerOrder {
    /// Leg 1's true role. Take-take crosses the book; the maker path rests.
    fn role(&self) -> &'static str {
        if self.strategy == "take-take" {
            "taker"
        } else {
            "maker"
        }
    }
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

/// One ATTEMPT at a hedge, kept so its fill can be recognised, credited to the
/// obligation it was placed for, and booked. Every attempt in a chain has its
/// own entry, and entries are never removed: a late frame on a superseded
/// attempt must stay recognisable, or it reads as unexplained money.
#[derive(Clone)]
struct HedgeOrder {
    /// The maker order whose fill created this hedge.
    maker_order_id: String,
    /// The OBLIGATION this attempt covers (`pending_hedges`' key) — stable
    /// across retries, so a fill on attempt 3 credits what attempt 1 owed.
    chain_id: String,
    market_id: String,
    venue: &'static str,
    side: &'static str,
    price: String,
    qty: i64,
    /// Cumulative contracts THIS attempt has been reported filled for. Venue
    /// reports are cumulative and arrive over several frames, so only the
    /// increase is credited — the same rule `arb_core::fill` applies to makers.
    cum_filled: i64,
}

/// The order side that TAKES `book_side` of the hedge leg's book: taking a bid
/// means SELLING (an ask-side order), taking an ask means BUYING. Written once
/// because the mint path and the retry path must never disagree about it.
fn taking_side(book_side: &str) -> &'static str {
    if book_side == "bid" {
        "ask"
    } else {
        "bid"
    }
}

/// What one hedge fill frame means. Pure, so the arithmetic that got I1/I2
/// wrong can be tested exhaustively without a runtime or a venue.
#[derive(Debug, PartialEq, Eq)]
struct HedgeCredit {
    /// Contracts newly filled by this frame. 0 = duplicate/stale/replayed.
    delta: i64,
    /// Of `delta`, the part the obligation still owed — what gets BOOKED.
    book: i64,
    /// Of `delta`, the part beyond the obligation. Contracts we hold with no
    /// maker leg to pair them with, so they are alarmed for a human rather
    /// than invented into a basket that never existed.
    over: i64,
    /// The obligation is covered — retire it.
    done: bool,
}

/// Credit one frame. `order_cum`/`order_qty` are the ATTEMPT's; `obligation` is
/// `(owed, filled)` for the chain, or `None` when the obligation has already
/// been retired — in which case anything new is an over-fill by definition.
fn hedge_credit(
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
    book_side: &str,
    touch: Option<&str>,
    last_try_at: std::time::Instant,
    now: std::time::Instant,
    ack_outstanding: bool,
) -> HedgePlan {
    if filled >= owed {
        return HedgePlan::Retire;
    }
    let since_try = now.saturating_duration_since(last_try_at);
    if since_try.as_secs_f64() < pol.interval_s {
        return HedgePlan::Hold;
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
fn first_attempt_acceptable(
    cx: &mut Cx,
    pol: Option<&HedgeRetry>,
    book_side: &str,
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

/// Which arm of the attribution one fill frame took.
///
/// Two gauges hang off this and they are NOT the same question: `fills` counts
/// frames belonging to a maker order of ours, while tape time advances for any
/// frame that is not a hedge's (which is what it did before the fill arm was
/// rewritten — a hedge fill has never advanced it). Returning a bool conflated
/// them and made `fills` over-report by every foreign fill on the account.
enum FillArm {
    /// Discharged (part of) a hedge obligation.
    Hedge,
    /// Belongs to a maker order of ours — a new fill or a replayed duplicate.
    Maker,
    /// Belongs to no order we know; held for its `order_ack`.
    Unattributed,
}

/// A fill the engine cannot attribute YET.
///
/// It is HELD, not dropped. A fill can beat its own `order_ack`: the ack is
/// emitted by the executor once the place's HTTP response returns (`exec.rs`),
/// while the fill arrives on the venue's private socket, and Kalshi's fill
/// channel carries only Kalshi's own order id — so until the ack lands there is
/// nothing to map it to. Dropping the frame there meant the basket was never
/// booked, nothing was credited, and the 5-second retry bought the hedge a
/// SECOND time; on take-take leg 1 no hedge was minted at all and `record_open`
/// never fired, so the concentration cap never bound.
///
/// What is still unclaimed past the grace is money that moved in our account
/// that we cannot explain, and it ALARMS. It is deliberately not fatal: the
/// Kalshi fill channel subscribes account-wide on purpose (`fills.rs`), so a
/// fill from a hand trade or another tool in this account is expected, and
/// halting on one would be a self-inflicted outage. It is also not credited to
/// any obligation — attributing money by guesswork is the one thing worse than
/// saying we cannot.
struct UnclaimedFill {
    venue: Venue,
    market_id: String,
    /// Highest cumulative count reported. Cumulative semantics make keeping the
    /// maximum equivalent to replaying every frame.
    cum: i64,
    /// MONOTONIC first-seen time, for the same reason `ParkedCancel::since` is:
    /// tape time stops advancing exactly when the feed dies, and this deadline
    /// must still fire. Re-parking keeps the ORIGINAL time.
    since: std::time::Instant,
}

/// How long a fill waits for the `order_ack` that would make it attributable
/// before the engine calls it unexplained money.
///
/// Same bound as `CANCEL_ACK_GRACE` and for the same reason: the place's HTTP
/// timeout is 15s (`main.rs`), so past that the ack has either arrived or never
/// will. It is longer than the 5s hedge-retry interval on purpose — the retry
/// holds while a fill on its market is unclaimed, so the grace is exactly how
/// long the engine will wait for proof before it risks hedging twice.
const FILL_ACK_GRACE: std::time::Duration = std::time::Duration::from_secs(15);

/// Held fills that have waited past the grace, in a deterministic order.
fn unclaimed_expired(
    park: &HashMap<String, UnclaimedFill>,
    now: std::time::Instant,
) -> Vec<String> {
    let mut out: Vec<String> = park
        .iter()
        .filter(|(_, u)| now.saturating_duration_since(u.since) >= FILL_ACK_GRACE)
        .map(|(k, _)| k.clone())
        .collect();
    out.sort();
    out
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
    let book = books.get(hedge.venue, &hedge.market_id).filter(|b| !b.is_crossed())?;
    let lvl = if side == "bid" { book.bids.first() } else { book.asks.first() }?;
    Some(HedgeAnchor {
        venue: hedge.venue.as_str(),
        market_id: hedge.market_id.clone(),
        side,
        price: lvl.price.clone(),
        ts,
    })
}

/// A cancel the engine has DECIDED on but cannot yet ADDRESS: the `order_ack`
/// carrying the venue's own order id for that order has not come back.
///
/// Parking it is the point. Both venues accept only THEIR id, so sending ours is
/// a no-op that both report as success (audit 2026-07-28: Kalshi 404 -> quirk K4
/// -> `Ok(())`, PM-US <300 for an id it never issued), and simply dropping the
/// cancel would leave the quote resting at a price the engine has already judged
/// wrong.
///
/// An entry is retired ONLY when a venue-addressed cancel has actually been
/// QUEUED for it. Not when the ack lands, not when it is escalated: a `try_send`
/// that hits a full channel loses the command, and retiring on a lost command
/// was a way to leave an unaddressable quote resting while the
/// `cancels_unresolved` gauge read healthy.
struct ParkedCancel {
    venue: Venue,
    market: String,
    /// MONOTONIC time the cancel was decided.
    ///
    /// Not tape time: the feed-stale pull is one of the callers, and tape time
    /// stops advancing exactly when the feed dies. Not wall time either — an NTP
    /// step backwards would freeze every parked cancel and a step forwards would
    /// expire them all at once, which is the escalation storm this deadline is
    /// rate-limited to avoid. `Instant` satisfies both requirements.
    since: std::time::Instant,
    /// Whether the client-id escalation has already gone out for this entry.
    /// One escalation per order: it costs a full paginated account read on
    /// Kalshi, and repeating it would be the 429 shape `PmusGateway::cancel`
    /// documents refusing.
    escalated: bool,
}

/// How long a cancel waits for its order's `order_ack` before the engine stops
/// expecting to learn the venue's id and escalates to a client-id cancel.
///
/// The place's HTTP timeout is 15s (`main.rs`), so past that the ack has either
/// arrived or never will. Escalating early costs one background order-list read;
/// escalating late leaves a stale quote resting and fillable. The one thing that
/// can escalate early is an executor backlogged behind its token bucket for
/// longer than that, which is its own alarm (`exec_dropped`, `chan_high_water`).
const CANCEL_ACK_GRACE: std::time::Duration = std::time::Duration::from_secs(15);

/// The one place a cancel command is built. `venue_oid` is the venue's OWN id.
fn cancel_by_venue_id(venue_oid: &str, market: &str) -> Action {
    Action::Cancel(CancelRequest {
        // PM-US REQUIRES the market slug in the cancel body and we refuse to
        // self-resolve it (that hammered the API into 429s); the intent carries
        // it, so it rides along here. Kalshi ignores it.
        market_slug: Some(market.to_string()),
        by: CancelBy::VenueId(venue_oid.to_string()),
    })
}

/// The venue-addressed cancel for OUR order id `oid`, or `None` when the venue's
/// id for it is not known yet — in which case the request is PARKED rather than
/// sent with an id the venue has never heard of.
///
/// `armed` false means no sink exists, so no place ever reaches a venue and no
/// `order_ack` ever comes back: parking would accumulate forever and prove
/// nothing. A dry run therefore addresses the cancel by our client id, which is
/// an honest statement of everything it knows, and the inert executor drops it.
fn resolve_cancel(
    oid: &str,
    market: &str,
    venue: Venue,
    armed: bool,
    oid_venue: &HashMap<String, String>,
    parked: &mut HashMap<String, ParkedCancel>,
    now: std::time::Instant,
) -> Option<Action> {
    if let Some(vid) = oid_venue.get(oid) {
        return Some(cancel_by_venue_id(vid, market));
    }
    if !armed {
        return Some(Action::Cancel(CancelRequest {
            by: CancelBy::ClientId(oid.to_string()),
            market_slug: Some(market.to_string()),
        }));
    }
    parked.entry(oid.to_string()).or_insert(ParkedCancel {
        venue,
        market: market.to_string(),
        since: now,
        escalated: false,
    });
    None
}

/// One item of work on the park, carrying everything the dispatch needs. Split
/// out so the policy is a pure function of (park, mappings, clock, killed) and
/// can be tested without a runtime.
#[derive(Debug, PartialEq, Eq)]
enum CancelWork {
    /// The venue's id is known now, so send the real cancel. Retires the entry
    /// once it is queued.
    Send { oid: String, venue: Venue, market: String, venue_order_id: String },
    /// The ack never came. Escalate to a client-id cancel, which Kalshi resolves
    /// against its own order list and PM-US refuses locally. The entry STAYS
    /// parked: a late ack must still be able to send the real cancel.
    Escalate { oid: String, venue: Venue, market: String },
}

impl CancelWork {
    fn venue(&self) -> Venue {
        match self {
            CancelWork::Send { venue, .. } | CancelWork::Escalate { venue, .. } => *venue,
        }
    }

    fn action(&self) -> Action {
        match self {
            CancelWork::Send { venue_order_id, market, .. } => {
                cancel_by_venue_id(venue_order_id, market)
            }
            CancelWork::Escalate { oid, market, .. } => Action::Cancel(CancelRequest {
                by: CancelBy::ClientId(oid.clone()),
                market_slug: Some(market.clone()),
            }),
        }
    }
}

/// Record the outcome of DISPATCHING one item of cancel work.
///
/// `queued` is `dispatch!`'s return: a `try_send` into a full channel LOSES the
/// command. Every state change therefore hangs off it —
///   * a `Send` retires the park entry only when the cancel is really on its way.
///     Retiring on a lost command dropped `cancels_unresolved` back to 0 while an
///     unaddressable quote went on resting, i.e. the one gauge built to surface
///     this read healthy;
///   * an `Escalate` burns the order's single escalation only when it was really
///     sent, so a lost one is retried on the next tick.
fn settle(parked: &mut HashMap<String, ParkedCancel>, work: &CancelWork, queued: bool) {
    if !queued {
        return;
    }
    match work {
        CancelWork::Send { oid, .. } => {
            parked.remove(oid);
        }
        CancelWork::Escalate { oid, .. } => {
            if let Some(p) = parked.get_mut(oid) {
                p.escalated = true;
            }
        }
    }
}

/// What the cancel deadline should do this tick, in dispatch order.
///
/// `Send` is unbounded — each is one targeted DELETE for a cancel we already owe,
/// and the parked set is bounded by the quotes whose cancel raced an ack.
/// `Escalate` is capped at ONE per tick, and skipped entirely while killed:
///   * a client-id cancel costs `all_orders()`, a paginated read of the FULL
///     order history, taken inside the venue executor's one-command-at-a-time
///     `spawn_blocking` — so a burst of them blocks every place and cancel for
///     that venue, and competes for the same Background budget as
///     `resting_order_ids`, which is the only evidence `cancel_all_and_verify`
///     accepts. Starving that turns a clean shutdown into "NOT CLEAN at exit".
///   * while killed, `Action::SweepAndVerify` is already queued and is a
///     strictly better remedy: it reaches orders we hold no id for at all, and
///     it proves the outcome.
fn cancel_work(
    parked: &HashMap<String, ParkedCancel>,
    oid_venue: &HashMap<String, String>,
    now: std::time::Instant,
    killed: bool,
) -> Vec<CancelWork> {
    let mut oids: Vec<&String> = parked.keys().collect();
    oids.sort(); // deterministic order for the log and the dispatch
    let mut out = Vec::new();
    let mut escalate: Option<CancelWork> = None;
    for oid in oids {
        let p = &parked[oid];
        if let Some(vid) = oid_venue.get(oid) {
            out.push(CancelWork::Send {
                oid: oid.clone(),
                venue: p.venue,
                market: p.market.clone(),
                venue_order_id: vid.clone(),
            });
            continue;
        }
        let expired = now.saturating_duration_since(p.since) >= CANCEL_ACK_GRACE;
        if expired && !p.escalated && !killed && escalate.is_none() {
            escalate = Some(CancelWork::Escalate {
                oid: oid.clone(),
                venue: p.venue,
                market: p.market.clone(),
            });
        }
    }
    out.extend(escalate);
    out
}

/// The effect commands one intent line implies, IN DISPATCH ORDER.
///
/// A place carrying `replaces` is an amend, and an amend is TWO effects: cancel
/// the order being replaced, then place the new one. The quoter emits it as one
/// intent because Python did the cancel inline through the gateway and used
/// `replaces` only to label the activity feed — so porting the intent stream
/// faithfully reproduced the RECORD of the amend and dropped its EFFECT. Every
/// superseded quote stayed live at a price the engine had already rejected
/// (audit 2026-07-28: 6785 of 8316 places in the 40-relationship shadow carry
/// `replaces`, against 850 cancels).
///
/// CANCEL FIRST, and the order is a deliberate choice. Both commands go into one
/// per-venue executor channel that is drained strictly sequentially, so
/// cancel-then-place leaves a window with NO quote of about one venue round
/// trip, while place-then-cancel leaves a window with TWO live quotes — both
/// fillable, one at a price we have already decided is wrong. The first costs a
/// missed fill; the second is exactly the adverse selection the reprice exists
/// to avoid. Python cancelled first for the same reason
/// (`src/arbbot/exec/quoter.py:311-313`).
///
/// A place whose cancel had to be PARKED still goes out. The quoter has already
/// moved its `resting` state and cannot be told otherwise (it is pure), so
/// withholding the place would leave it believing it rests a quote that does not
/// exist — the "market goes quietly dark" failure the order-id seeding comment
/// below describes. Two live quotes for the length of the parked cancel is the
/// lesser harm, and it is counted (`cancels_unresolved`) rather than silent.
fn intent_actions(
    v: &serde_json::Value,
    venue: Venue,
    armed: bool,
    oid_venue: &HashMap<String, String>,
    parked: &mut HashMap<String, ParkedCancel>,
    now: std::time::Instant,
) -> Vec<Action> {
    let mut out: Vec<Action> = Vec::new();
    let oid = v.get("order_id").and_then(|x| x.as_str());
    if let (Some(market), Some(oid)) = (v.get("place").and_then(|x| x.as_str()), oid) {
        if let Some(roid) = v.get("replaces").and_then(|x| x.as_str()) {
            if let Some(a) = resolve_cancel(roid, market, venue, armed, oid_venue, parked, now) {
                out.push(a);
            }
        }
        // The quoter only ever rests post-only GTC makers, so tif/post_only are
        // fixed here; a taker hedge carries its own.
        // `client_order_id` is our own order id, which is what makes a retried
        // place idempotent at the venue.
        let taker = v.get("taker").and_then(|x| x.as_bool()).unwrap_or(false);
        out.push(Action::Place(PlaceRequest {
            market: market.to_string(),
            side: match v.get("side").and_then(|x| x.as_str()) {
                Some("ask") => VenueSide::Ask,
                _ => VenueSide::Bid,
            },
            price: v.get("price").and_then(|x| x.as_str()).unwrap_or("0").to_string(),
            qty: v.get("count").and_then(|x| x.as_i64()).unwrap_or(0),
            tif: if taker { Tif::Ioc } else { Tif::Gtc },
            post_only: !taker,
            client_order_id: oid.to_string(),
        }));
    } else if let (Some(market), Some(oid)) = (v.get("cancel").and_then(|x| x.as_str()), oid) {
        if let Some(a) = resolve_cancel(oid, market, venue, armed, oid_venue, parked, now) {
            out.push(a);
        }
    }
    out
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
    // Order-id counters (m = maker, t = take-take, h = hedge). They must not
    // restart at 0 on a LIVE run: the id is sent as the venue's
    // client_order_id, and Kalshi rejects one it has seen before with
    // `409 order_already_exists`. Observed 2026-07-28 right after a restart —
    // 4 places rejected. The rejection is the small half: the engine registers
    // an order at INTENT time, before the place result, so it believed those
    // quotes were resting when the venue had never accepted them. Those
    // markets then go quietly dark, because the engine sees no reason to
    // re-quote something it thinks is already working.
    //
    // Seeding from the wall clock keeps ids monotonic across restarts without
    // changing the id FORMAT, which the golden digest tests pin. bench/replay
    // still start at 0, so those digests stay byte-exact.
    let id_base: u64 = if cfg.bench { 0 } else { wall_now() as u64 * 1000 };
    let mut next_tt_oid: u64 = id_base;
    // The bar is re-derived from marks on the stats tick: it moves as the book
    // turns over, and a stale bar is a wrong bar in both directions.
    //
    // `None` is a REFUSAL to run take-take, not a missing number (see
    // `taketake::Bar`). It used to be `unwrap_or(DEFAULT_BAR_APR)` over a plain
    // read, so when marks.json froze at 12:46:12 on 2026-07-28 the armed
    // session spent four hours firing against that frozen 10.0088%/yr bar and
    // said nothing about it.
    let mut tt_bar: Option<f64> = None;
    if let Some(tt) = cfg.take_take.as_ref() {
        let bar = read_bar(&tt.marks_path);
        tt_bar = bar.tradable();
        eprintln!(
            "[take-take] {} — {}, cap {}ct/rel, clip {}",
            if tt.detect_only { "DETECT ONLY (places nothing)" } else { "ARMED" },
            bar.describe(),
            tt.max_ct_per_rel,
            tt.max_clip
        );
    }
    let mut next_oid: u64 = id_base;
    let mut intents: Vec<String> = Vec::new();
    let mut killed = false;
    // Feed-health pull (card 0a7e5478). Holds the REASON, not just a flag, so
    // a pulled engine can always say why it is silent. Starts pulled when the
    // check is on: we have not yet proven the feeds are healthy, and the first
    // tick either clears it or names the problem.
    let mut feed_reason: Option<String> =
        cfg.health_file.is_some().then(|| "startup — feeds not yet proven healthy".to_string());
    // The engine's own subscription to the recorder, tracked separately from
    // what the recorder says about the venues: a bench tape cannot disconnect,
    // a socket can and did (ten times on 2026-07-28).
    let mut link = if cfg.bench { Link::Fresh } else { Link::Down };
    // The health-file keys we require evidence for — the venues we quote.
    let required = required_feeds(&by_market);
    let mut last_now: f64 = 0.0;
    let mut chan_hw: usize = 0;
    let mut fills = FillLedger::new();
    // order id -> (relationship id, class). A fill arrives with our order id
    // only, but exposure is booked per relationship, so the mapping is captured
    // at place time when the rel is in hand.
    let mut order_rel: HashMap<String, MakerOrder> = HashMap::new();
    // venue's order id -> ours, learned from order_ack. Read by the FILL path:
    // a venue reports a fill under its own id.
    let mut venue_oid: HashMap<String, String> = HashMap::new();
    // ...and ours -> the venue's, learned from the same ack. Read by the CANCEL
    // path: both venues' cancel endpoints accept only their own id, and until
    // 2026-07-28 this map did not exist, so every per-order cancel the engine
    // ever sent was addressed to an id the venue had never issued.
    let mut oid_venue: HashMap<String, String> = HashMap::new();
    // Cancels waiting on the ack that will make them addressable, by OUR id.
    let mut parked_cancels: HashMap<String, ParkedCancel> = HashMap::new();
    let mut n_cancel_escalated: u64 = 0;
    // Every hedge ATTEMPT we placed, by OUR id — superseded ones included, so a
    // late frame on one is still recognisable. Deliberately separate from the
    // FillLedger: a hedge in the ledger would hedge its own fill.
    let mut hedge_orders: HashMap<String, HedgeOrder> = HashMap::new();
    let mut next_hedge_oid: u64 = id_base;
    // Outstanding hedge OBLIGATIONS, keyed by the id of the first attempt made
    // for each (see `PendingHedge` for the invariant it maintains). Populated
    // for every obligation, retry policy or not: it is the accounting unit, and
    // `hedged_by_maker` — one credit shared by every obligation of a maker
    // order — is what it replaces.
    let mut pending_hedges: HashMap<String, PendingHedge> = HashMap::new();
    // Fills we cannot attribute yet, by the id the venue reported. See
    // `UnclaimedFill`: held for their ack, alarmed if it never comes.
    let mut unclaimed_fills: HashMap<String, UnclaimedFill> = HashMap::new();
    let (mut n_retry, mut n_naked) = (0u64, 0u64);
    // Fills that expired unclaimed (money we cannot explain) and hedge fills
    // beyond what an obligation owed. Both must stay 0.
    let (mut n_unattributed, mut n_overhedge) = (0u64, 0u64);
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
    let mut cancel_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    cancel_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut fill_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    fill_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    /// Queue one effect command on its venue's executor. Returns false when the
    /// channel is full — the command is LOST and only the counter moves, which
    /// is why callers with an ordered sequence must stop on a false.
    macro_rules! dispatch {
        ($venue:expr, $action:expr) => {{
            match exec_txs.get(&$venue) {
                Some(tx) => {
                    let queued = tx
                        .try_send(ExecCmd { t_read: std::time::Instant::now(), action: $action })
                        .is_ok();
                    if !queued {
                        exec_stats.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    queued
                }
                None => false,
            }
        }};
    }

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
                                    // The intent already carries who emitted
                                    // it; the ledger just has to stop
                                    // discarding that.
                                    strategy: match v.get("tag").and_then(|x| x.as_str()) {
                                        Some("take-take") => "take-take",
                                        _ => "maker-hedge",
                                    },
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
                    // Build the REAL requests from the intent (see
                    // `intent_actions`: an amend is a cancel AND a place, in
                    // that order).
                    if let Some(venue) =
                        v.get("venue").and_then(|x| x.as_str()).and_then(Venue::parse)
                    {
                        // Only the park reads this clock, and only an armed engine
                        // parks, so a replay's determinism cannot depend on it.
                        let now = std::time::Instant::now();
                        for action in intent_actions(
                            &v,
                            venue,
                            cfg.armed,
                            &oid_venue,
                            &mut parked_cancels,
                            now,
                        ) {
                            if !dispatch!(venue, action) {
                                // The commands of ONE intent are a sequence: an
                                // amend's place must never go out without the
                                // cancel that precedes it, or the amend doubles
                                // the exposure it was meant to move.
                                eprintln!(
                                    "[engine] executor {venue:?} backlogged — dropped the \
                                     remaining command(s) of this intent"
                                );
                                break;
                            }
                        }
                    }
                }
            }
        };
    }

    // Stop quoting AND leave nothing resting — the same standard the KILL path
    // was held to on 2026-07-28, for the same reason.
    //
    // `cancel_all` alone is not enough: it reaches only orders the engine still
    // holds ids for, and NONE of its cancels is verified. The feed-stale pull
    // did exactly that and no more, so `feed_pulled: true` could sit over real
    // quotes resting on a book the engine could no longer see — the one state
    // this pull exists to prevent. The venue sweep is the part that cannot be
    // fooled: it reaches orders we hold no id for at all, and it PROVES the
    // outcome.
    macro_rules! pull_quotes {
        ($why:expr) => {{
            for q in quoters.iter_mut() {
                q.cancel_all(&mut cx, last_now, &mut intents);
                drain_intents!(Some(&q.rel));
            }
            // Empty unarmed — an unarmed engine has no executor and nothing at
            // a venue to sweep.
            for (venue, tx) in exec_txs.iter() {
                if tx
                    .try_send(ExecCmd {
                        t_read: std::time::Instant::now(),
                        action: Action::SweepAndVerify,
                    })
                    .is_err()
                {
                    eprintln!(
                        "[engine] {}: could not queue sweep for {venue:?} — executor \
                         backlogged; book NOT proven clean",
                        $why
                    );
                }
            }
        }};
    }

    // Attribute ONE fill frame to the order it belongs to, and act on it.
    //
    // Returns which arm it took (`FillArm`), because the caller's two gauges ask
    // different questions: `fills` counts maker frames only, and tape time
    // advances for anything that is not a hedge frame.
    //
    // Called from the `fill` arm and from the `order_ack` arm, which replays a
    // held fill the moment its ack makes it addressable. `$since` is the
    // MONOTONIC time the frame was first seen, so a frame going back on the
    // hold keeps its original deadline rather than restarting it.
    //
    // This is one macro rather than two copies because the two callers must
    // never disagree about what a fill means.
    macro_rules! attribute_fill {
        ($oid:expr, $cum:expr, $venue:expr, $market:expr, $now:expr, $since:expr) => {{
            let oid: &str = $oid;
            let cum: i64 = $cum;
            let now: f64 = $now;
            // A hedge fill discharges (part of) an obligation. Hedges are never
            // in the FillLedger — registering one would let its own fill mint
            // another hedge, forever — so they are recognised here.
            match hedge_orders.get(oid).cloned() {
                Some(h) => {
                    let ob = pending_hedges.get(&h.chain_id).map(|p| (p.owed, p.filled));
                    let c = hedge_credit(cum, h.qty, h.cum_filled, ob);
                    if c.delta > 0 {
                        // I1/I2: credit the ATTEMPT (so its own later frames are
                        // deltas) and the OBLIGATION (so the retry knows what is
                        // left). Retiring the attempt on its first frame is what
                        // booked a 10-lot as a 4-lot and lost the rest.
                        if let Some(o) = hedge_orders.get_mut(oid) {
                            o.cum_filled += c.delta;
                        }
                        if let Some(p) = pending_hedges.get_mut(&h.chain_id) {
                            p.filled += c.delta;
                        }
                        if c.book > 0 {
                            match order_rel.get(&h.maker_order_id) {
                                Some(mo) => {
                                    if let Some(lp) = cfg.ledger_path.as_deref() {
                                        book_basket(lp, mo, &h, c.book, now);
                                    }
                                    eprintln!(
                                        "[ledger] booked {} x{} ({} maker / {} hedge)",
                                        mo.rel_id, c.book, mo.market_id, h.market_id
                                    );
                                }
                                // Without the maker order there is no relationship
                                // id, so there is no honest ledger record to write
                                // — and an unbooked basket is exposure no restart
                                // can see. `order_rel` is never pruned, so this is
                                // a bug in the engine rather than a venue event.
                                None => eprintln!(
                                    "[ledger] CANNOT BOOK {}x {} ({} hedge {oid}): maker order \
                                     {} is unknown to this engine — RECOVER BY HAND.",
                                    c.book, h.market_id, h.venue, h.maker_order_id
                                ),
                            }
                        }
                        if c.over > 0 {
                            // Contracts with no maker leg to pair them with:
                            // the obligation was already covered (a superseded
                            // IOC filled late). Booking them as a basket would
                            // invent a maker fill that never happened, so they
                            // are alarmed instead.
                            n_overhedge += 1;
                            eprintln!(
                                "[hedge] OVER-HEDGED: {} extra contract(s) filled on {} \
                                 ({} {}) beyond what obligation {} owed. This is directional \
                                 exposure the OPPOSITE way and it is NOT booked as a basket \
                                 — RECONCILE BY HAND.",
                                c.over, oid, h.venue, h.market_id, h.chain_id
                            );
                        }
                        if c.done {
                            // I3: retired only now that it is really covered.
                            // Its attempts stay in `hedge_orders` so a further
                            // frame on one is recognised as an over-fill rather
                            // than as money we cannot explain.
                            pending_hedges.remove(&h.chain_id);
                        }
                    }
                    FillArm::Hedge
                }
                None => match fills.observe_cum_fill(oid, cum) {
                    arb_core::fill::FillOutcome::Minted(ob) => {
                        // Book the new exposure BEFORE the hedge intent, so
                        // the next quote sees capital this fill just spent.
                        if let (Some(rv), Some(mo)) = (cfg.risk.as_ref(), order_rel.get(oid)) {
                            rv.record_open(&mo.rel_id, mo.class, ob.qty() as f64);
                        }
                        // No anchor => no hedge target. The obligation is
                        // deliberately left unconsumed so the ledger's
                        // dropped_unconsumed() alarm surfaces it instead of
                        // an exposed leg vanishing silently. A crossed hedge
                        // book lands here too (see `hedge_anchor`).
                        if let Some(a) = ob.anchor().cloned() {
                            let (f_oid, _order_market, qty, _) = ob.into_parts();
                            n_hedge += 1;
                            intents.push(
                                json!({"hedge_needed": a.market_id, "order_id": f_oid.clone(),
                                       "qty": qty, "anchor_price": a.price.clone(), "ts": now})
                                .to_string(),
                            );
                            // The obligation says WHAT to hedge; the order
                            // below is the one that does it. Marketable IOC,
                            // not a resting quote: an unhedged leg is unbounded
                            // directional risk until resolution, so the hedge
                            // crosses rather than waits.
                            //
                            // `a.side` is the hedge-leg BOOK side we take.
                            let order_side = taking_side(a.side);
                            // Price at the CURRENT touch so it actually fills.
                            // The anchor is the fallback: it was captured at
                            // place time precisely because the burst that fills
                            // you is the burst that gaps your book, so a missing
                            // level here is exactly when it is needed.
                            let px = Venue::parse(a.venue)
                                .and_then(|vn| books.get(vn, &a.market_id))
                                .and_then(|b| {
                                    if a.side == "bid" { b.bids.first() } else { b.asks.first() }
                                })
                                .map(|l| l.price.clone())
                                .unwrap_or_else(|| a.price.clone());
                            next_hedge_oid += 1;
                            let hoid = format!("h{next_hedge_oid}");
                            // The obligation, named by its FIRST attempt. It is
                            // recorded BEFORE the placement decision, so a
                            // refusal below still leaves a tracked, alarmed
                            // obligation rather than a leg nobody owns.
                            // MONOTONIC, not the fill's tape time: the fill feed
                            // is a separate socket from the market feed, so tape
                            // time can be frozen at this very moment (see
                            // `PendingHedge::first_at`).
                            let at = std::time::Instant::now();
                            pending_hedges.insert(
                                hoid.clone(),
                                PendingHedge {
                                    maker_order_id: f_oid.clone(),
                                    owed: qty,
                                    filled: 0,
                                    anchor: a.clone(),
                                    first_at: at,
                                    last_try_at: at,
                                    latest_attempt: None,
                                    tries: 0,
                                    alarmed: false,
                                    hold_logged: false,
                                },
                            );
                            // I5, first attempt included — see
                            // `first_attempt_acceptable` for why it is gated at
                            // all and why bench/replay is the one exception.
                            let acceptable = first_attempt_acceptable(
                                &mut cx,
                                cfg.hedge_retry.as_ref(),
                                a.side,
                                &px,
                                &a.price,
                            );
                            if acceptable {
                                if let Some(p) = pending_hedges.get_mut(&hoid) {
                                    p.tries = 1;
                                    p.latest_attempt = Some(hoid.clone());
                                }
                                hedge_orders.insert(
                                    hoid.clone(),
                                    HedgeOrder {
                                        maker_order_id: f_oid,
                                        chain_id: hoid.clone(),
                                        market_id: a.market_id.clone(),
                                        venue: a.venue,
                                        side: order_side,
                                        price: px.clone(),
                                        qty,
                                        cum_filled: 0,
                                    },
                                );
                                intents.push(
                                    json!({"ts": now, "place": a.market_id, "venue": a.venue,
                                           "side": order_side, "price": px, "count": qty,
                                           "order_id": hoid, "tag": "hedge", "taker": true})
                                    .to_string(),
                                );
                            } else {
                                let (slip, alarm_s) = cfg
                                    .hedge_retry
                                    .as_ref()
                                    .map(|p| (p.max_slip.as_str(), p.alarm_after_s))
                                    .unwrap_or(("0", 0.0));
                                eprintln!(
                                    "[hedge] WAIT {qty}x {} on {} — the touch {px} is worse \
                                     than the anchor {} by more than {slip} ({} side). \
                                     Obligation {hoid} is live and retrying; NOTHING is hedged \
                                     yet, and it alarms as NAKED if it is still open in \
                                     {alarm_s:.0}s.",
                                    a.market_id, a.venue, a.price, a.side
                                );
                            }
                            drain_intents!(Option::<&Rel>::None);
                        }
                        FillArm::Maker
                    }
                    // Duplicate / stale / replayed report for an order we know:
                    // idempotent by construction, nothing to do.
                    arb_core::fill::FillOutcome::Seen => FillArm::Maker,
                    arb_core::fill::FillOutcome::Unknown => {
                        // HELD, not dropped. See `UnclaimedFill` — this is the
                        // fill the engine used to count in `fills` and then
                        // throw away with no alarm at all.
                        if !unclaimed_fills.contains_key(oid) {
                            eprintln!(
                                "[fill] UNATTRIBUTED {cum}x on {} {} reported as order {oid} \
                                 — no order of ours maps to that id. Holding it for its \
                                 order_ack; it alarms in {}s if none comes.",
                                $venue.as_str(),
                                $market,
                                FILL_ACK_GRACE.as_secs()
                            );
                        }
                        let e = unclaimed_fills.entry(oid.to_string()).or_insert(UnclaimedFill {
                            venue: $venue,
                            market_id: $market.to_string(),
                            cum,
                            since: $since,
                        });
                        e.cum = e.cum.max(cum);
                        // NOT a maker frame. It was counted as one, so the `fills`
                        // gauge over-reported by every foreign fill on the account —
                        // and a frame replayed out of the hold would have been
                        // counted a second time. `fills_unclaimed` and
                        // `fills_unattributed` are where this frame is visible.
                        FillArm::Unattributed
                    }
                },
            }
        }};
    }

    macro_rules! summary {
        () => {{
            let elapsed = t_start.elapsed().as_secs_f64();
            serde_json::json!({
                "mode": if cfg.bench { "bench" } else if cfg.armed { "live" } else { "shadow" },
                "events": n_ev, "book_events": n_book, "intents": n_int,
                "take_take_found": n_tt, "take_take_bar_apr": tt_bar,
                "take_take_gated": n_tt_gated, "take_take_fired": n_tt_fired,
                "killed": killed,
                "feed_pulled": feed_reason.is_some(),
                "risk_allowed": cfg.risk.as_ref().map(|r| r.stats().0).unwrap_or(0),
                "risk_rejected": cfg.risk.as_ref().map(|r| r.stats().1).unwrap_or(0),
                // Cancels the engine decided on but has not been able to queue a
                // venue-addressed command for. Healthy is 0, or a transient 1
                // while an ack is in flight.
                "cancels_unresolved": parked_cancels.len(),
                // ...of which these have already had their one client-id
                // escalation and are still unaddressable. This is the subset a
                // human has to reason about, and it is separated out precisely so
                // the gauge above stays a real signal: the commonest member is a
                // place the venue REJECTED, where nothing rests and nothing is
                // wrong (see the escalation log line).
                "cancels_unaddressable":
                    parked_cancels.values().filter(|p| p.escalated).count(),
                "cancels_escalated": n_cancel_escalated,
                "hedges_pending": pending_hedges.len(),
                "hedges_retried": n_retry,
                "hedges_naked": n_naked,
                // Hedge contracts filled beyond what an obligation owed — a
                // position with no maker leg to pair it with. Must stay 0.
                "hedges_overfilled": n_overhedge,
                "order_acks": n_ack, "fills": n_fill, "hedge_obligations": n_hedge,
                // Fills held for the `order_ack` that would name them. A
                // transient 1 is the ack race; a persistent count is a broken
                // ack path.
                "fills_unclaimed": unclaimed_fills.len(),
                // ...and fills that gave up waiting: money that moved in our
                // account that we cannot explain. Must stay 0.
                "fills_unattributed": n_unattributed,
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
                // Feed-CONNECTION control lines (crate::feed). They ride the
                // same ordered channel as book events precisely so the pull
                // lands where the outage did, and they carry no venue/market so
                // they would otherwise fall out of the parse below.
                //
                // LIVE ONLY. A bench tape and a WAL replay must stay
                // byte-deterministic, and re-enacting a recorded outage would
                // make their decisions depend on `Instant::now()`; a replay
                // reads these lines as the incident record they are.
                if !cfg.bench && (kind == crate::feed::FEED_UP || kind == crate::feed::FEED_DOWN) {
                    if kind == crate::feed::FEED_UP {
                        // NOT a clear. `resync_reason` decides when the welcome
                        // burst has actually made the books current again; this
                        // only starts the clock for it.
                        link = Link::Resyncing {
                            since: std::time::Instant::now(),
                            snapshots: 0,
                        };
                        eprintln!(
                            "[engine] feed reconnected — quotes stay pulled until the welcome \
                             snapshot burst has landed"
                        );
                    } else {
                        // Act on the DOWN EDGE only: `socket_feed` re-emits
                        // FEED_DOWN every 2s for as long as the recorder is
                        // unreachable.
                        let edge = !matches!(link, Link::Down);
                        link = Link::Down;
                        if edge {
                            // ...and sweep only on the way IN, the same rule the
                            // health tick follows. Nothing can have started
                            // resting while the engine was already pulled, and a
                            // flapping subscriber would otherwise sweep every
                            // reconnect against the rate budget the order path
                            // needs.
                            let entering = feed_reason.is_none();
                            feed_reason = resync_reason(&link, std::time::Instant::now());
                            eprintln!(
                                "[engine] FEED DOWN ({}) — quotes pulled",
                                v.get("note").and_then(|x| x.as_str()).unwrap_or("no reason given")
                            );
                            if entering {
                                pull_quotes!("FEED DOWN");
                            }
                        }
                    }
                    continue;
                }
                let Some(venue) = v.get("venue").and_then(|x| x.as_str()).and_then(Venue::parse)
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
                        // Evidence that a reconnect's welcome burst is really
                        // arriving — the only thing that clears the pull a
                        // disconnect set (see `Link`).
                        if let Link::Resyncing { snapshots, .. } = &mut link {
                            *snapshots += 1;
                        }
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
                        // never fire — and a CANCEL cannot be addressed at all,
                        // because both venues accept only their own id.
                        if let (Some(ours), Some(theirs)) = (
                            v.get("order_id").and_then(|x| x.as_str()),
                            v.get("venue_order_id").and_then(|x| x.as_str()),
                        ) {
                            venue_oid.insert(theirs.to_string(), ours.to_string());
                            oid_venue.insert(ours.to_string(), theirs.to_string());
                            // ...and THE moment a fill that beat this ack becomes
                            // attributable. A hedge is an IOC, so it fills in the
                            // instant it is accepted and its fill frame can
                            // overtake the ack that names it (observed margin
                            // 48 ms). Dropping that frame left the basket
                            // unbooked, the obligation credited 0, and the
                            // 5-second retry bought the hedge a second time —
                            // 10 Kalshi long against 5 PM short. Replay it here,
                            // before any deadline can act on the wrong state.
                            if let Some(u) = unclaimed_fills.remove(theirs) {
                                eprintln!(
                                    "[fill] {theirs} is {ours} — replaying the held fill of {} \
                                     that arrived before its ack",
                                    u.cum
                                );
                                let ts = ts_local_ns as f64 / 1e9;
                                let _ = attribute_fill!(
                                    ours, u.cum, u.venue, &u.market_id, ts, u.since
                                );
                            }
                            // THE moment a cancel decided before this ack became
                            // addressable. It was parked rather than sent with
                            // our id (a no-op both venues report as success), so
                            // this is where it actually goes out.
                            //
                            // The park entry is retired by `settle` only if the
                            // command was actually QUEUED — never before the
                            // dispatch. A full channel loses the command, and
                            // logging a send that never happened while the gauge
                            // dropped to 0 was how an unaddressable quote could
                            // rest with every number reading healthy.
                            let w = parked_cancels.get(ours).map(|p| CancelWork::Send {
                                oid: ours.to_string(),
                                venue: p.venue,
                                market: p.market.clone(),
                                venue_order_id: theirs.to_string(),
                            });
                            if let Some(w) = w {
                                let queued = dispatch!(w.venue(), w.action());
                                settle(&mut parked_cancels, &w, queued);
                                if queued {
                                    eprintln!(
                                        "[engine] cancel {ours} was waiting on its ack — sent to \
                                         {} as {theirs}",
                                        w.venue().as_str()
                                    );
                                } else {
                                    eprintln!(
                                        "[engine] cancel {ours} is addressable now ({theirs}) but \
                                         the {} executor is FULL — NOT sent; the order is still \
                                         resting and the cancel stays owed",
                                        w.venue().as_str()
                                    );
                                }
                            }
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
                        let oid: String = venue_oid
                            .get(reported)
                            .cloned()
                            .unwrap_or_else(|| reported.to_string());
                        let now = ts_local_ns as f64 / 1e9;
                        let arm = attribute_fill!(
                            &oid,
                            cum,
                            venue,
                            &market_id,
                            now,
                            std::time::Instant::now()
                        );
                        if matches!(arm, FillArm::Maker) {
                            n_fill += 1;
                        }
                        if !matches!(arm, FillArm::Hedge) {
                            last_now = now;
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
                        //
                        // No trustworthy bar, no take-take. `tt_bar` is `None`
                        // exactly when `marks.json` is present but stale or
                        // corrupt, and the bar IS the profitability test — a
                        // frozen one is wrong in both directions, so there is no
                        // substitute to fall back to.
                        if let (Some(tt), Some(tt_bar)) = (cfg.take_take.as_ref(), tt_bar) {
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
                                let c = match found {
                                    // A venue offering below its own bid means
                                    // OUR book is corrupt, and every price
                                    // derived from it is fiction. The detector
                                    // has refused it since 4542e5f, but the
                                    // engine DISCARDED the reason, so six hours
                                    // of live book corruption on
                                    // KXRATECUT-26DEC31 (2026-07-28) logged
                                    // nothing at all.
                                    Err(crate::taketake::Skip::CrossedBook { venue }) => {
                                        intents.push(json!({"ts": now,
                                            "skip": [format!("crossed book {venue} take-take {}",
                                                             quoters[qi].rel.id)]})
                                            .to_string());
                                        drain_intents!(Some(&quoters[qi].rel));
                                        continue;
                                    }
                                    Err(_) => continue,
                                    Ok(c) => c,
                                };
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
                    // Those cancels reach only orders we still hold ids for,
                    // and NONE of them is verified. On 2026-07-28 that path
                    // logged "cancelled" for a PM-US order that was still
                    // resting 35 minutes later, which is how the engine can
                    // report itself halted while it is still exposed.
                    //
                    // Follow with a real venue sweep that proves the book is
                    // empty. Halting is the one moment where "probably
                    // cancelled" is not good enough.
                    for (venue, tx) in exec_txs.iter() {
                        if tx
                            .try_send(ExecCmd {
                                t_read: std::time::Instant::now(),
                                action: Action::SweepAndVerify,
                            })
                            .is_err()
                        {
                            eprintln!(
                                "[engine] KILL: could not queue sweep for {venue:?} — \
                                 executor backlogged; book NOT proven clean"
                            );
                        }
                    }
                } else if !kill_now && killed {
                    killed = false;
                    eprintln!("[engine] KILL switch cleared — quoting resumes");
                }
            }
            _ = hedge_iv.tick(), if cfg.hedge_retry.is_some() && !cfg.bench => {
                let pol = cfg.hedge_retry.as_ref().expect("guarded above");
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
                let mut plans: Vec<(String, HedgePlan, Option<String>)> = Vec::new();
                for (chain, p) in pending_hedges.iter() {
                    // The side of the hedge leg's book we TAKE. Read off the
                    // OBLIGATION's anchor, not off the last attempt: the anchor
                    // is the one thing about this obligation that never moves.
                    let book_side = p.anchor.side;
                    let hedge_book = Venue::parse(p.anchor.venue)
                        .and_then(|vn| books.get(vn, &p.anchor.market_id));
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
                        .and_then(|b| {
                            if book_side == "bid" { b.bids.first() } else { b.asks.first() }
                        })
                        .map(|l| l.price.clone());
                    // Does THIS obligation have an attempt at the venue whose
                    // `order_ack` has not landed? Only then can an unattributed
                    // fill on its market plausibly be its own. An obligation whose
                    // first attempt the slip gate refused has nothing at the venue,
                    // so holding it would be added naked time bought for nothing.
                    let ack_outstanding = p
                        .latest_attempt
                        .as_ref()
                        .is_some_and(|a| !oid_venue.contains_key(a))
                        && unclaimed_fills.values().any(|u| {
                            u.market_id == p.anchor.market_id
                                && Venue::parse(p.anchor.venue) == Some(u.venue)
                        });
                    let plan = hedge_plan(
                        &mut cx,
                        pol,
                        p.owed,
                        p.filled,
                        &p.anchor.price,
                        book_side,
                        touch.as_deref(),
                        p.last_try_at,
                        mono,
                        ack_outstanding,
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
                            p.anchor.venue,
                            mono.saturating_duration_since(p.first_at).as_secs_f64(),
                            p.tries,
                            p.anchor.price,
                            book_side,
                            pol.max_slip,
                        )
                    });
                    plans.push((chain.clone(), plan, alarm));
                }
                plans.sort_by(|a, b| a.0.cmp(&b.0)); // deterministic dispatch order
                for (chain, plan, alarm) in plans {
                    if let Some(msg) = alarm {
                        if let Some(p) = pending_hedges.get_mut(&chain) {
                            p.alarmed = true;
                        }
                        n_naked += 1;
                        eprintln!("{msg}");
                    }
                    match plan {
                        // Not due, or the book will not offer a profitable price.
                        // `last_try_at` is NOT bumped on a wait: it gates
                        // PLACEMENTS, and looking at the book again next tick is
                        // free, so a naked leg hedges the moment the price comes
                        // back instead of up to `interval_s` later.
                        HedgePlan::Hold | HedgePlan::Wait => {}
                        // Deferring to a fill we cannot attribute. Said out loud
                        // once per obligation: `HedgePlan::Hold` used to swallow
                        // this, so added naked time had no signal at all.
                        HedgePlan::HoldForAck => {
                            let Some(p) = pending_hedges.get_mut(&chain) else { continue };
                            if !p.hold_logged {
                                p.hold_logged = true;
                                eprintln!(
                                    "[hedge] HOLD {}x {} on {} — a fill on that market is not \
                                     attributable yet and may be this hedge's own (attempt {}, \
                                     no order_ack). Not re-placing for up to {}s; if \
                                     fills_unattributed rises, CHECK FOR A DUPLICATE HEDGE.",
                                    p.owed - p.filled,
                                    p.anchor.market_id,
                                    p.anchor.venue,
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
                            pending_hedges.remove(&chain);
                        }
                        HedgePlan::Retry { qty, price } => {
                            next_hedge_oid += 1;
                            let hoid = format!("h{next_hedge_oid}");
                            let Some(p) = pending_hedges.get_mut(&chain) else { continue };
                            p.tries += 1;
                            p.last_try_at = mono;
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
                            };
                            n_retry += 1;
                            eprintln!(
                                "[hedge] retry {hoid} {qty}x {} @ {price} (try {tries}; \
                                 obligation {chain} owed {owed}, {filled} hedged)",
                                ho.market_id
                            );
                            intents.push(
                                json!({"ts": last_now, "place": ho.market_id,
                                       "venue": ho.venue, "side": ho.side,
                                       "price": ho.price, "count": ho.qty,
                                       "order_id": hoid, "tag": "hedge", "taker": true,
                                       "retry": tries})
                                .to_string(),
                            );
                            // EVERY attempt stays in `hedge_orders`, superseded
                            // ones included: an IOC that filled late still has to
                            // credit its obligation, and the obligation's key does
                            // not move, so the credit lands on the right one.
                            hedge_orders.insert(hoid, ho);
                            drain_intents!(Option::<&Rel>::None);
                        }
                    }
                }
            }
            // Fills held for an `order_ack` that has not come. Bench has no live
            // ack path at all and must stay byte-deterministic, so it relies on
            // the flush after the loop instead of this deadline.
            _ = fill_iv.tick(), if !cfg.bench => {
                for id in unclaimed_expired(&unclaimed_fills, std::time::Instant::now()) {
                    let Some(u) = unclaimed_fills.remove(&id) else { continue };
                    n_unattributed += 1;
                    eprintln!(
                        "[fill] UNEXPLAINED {}x on {} {} reported as order {id} — no order_ack \
                         claimed it within {}s. Either a fill on an order of ours whose ack was \
                         lost (a place can return a parse error and still rest) or a fill from \
                         outside this engine — the Kalshi fill channel is account-wide by \
                         design. It is NOT credited to any hedge: attributing money by guess is \
                         worse than saying we cannot. RECONCILE BY HAND.",
                        u.cum,
                        u.venue.as_str(),
                        u.market_id,
                        FILL_ACK_GRACE.as_secs()
                    );
                }
            }
            // Cancels the engine owes but could not address when it decided on
            // them. Only an armed engine can ever learn a venue id, so only an
            // armed engine parks (see `resolve_cancel`) and only it has anything
            // to do here. `cancel_work` owns the policy — including the one
            // escalation per tick and none at all while killed.
            _ = cancel_iv.tick(), if cfg.armed => {
                for w in cancel_work(&parked_cancels, &oid_venue, std::time::Instant::now(), killed)
                {
                    let queued = dispatch!(w.venue(), w.action());
                    settle(&mut parked_cancels, &w, queued);
                    if let (CancelWork::Escalate { oid, venue, market }, true) = (&w, queued) {
                        n_cancel_escalated += 1;
                        // Deliberately not shouted. The commonest cause is a
                        // place the venue REJECTED (post-only would cross, rate
                        // limited, 409 duplicate id): the quoter still believes
                        // it rests, so its next reprice parks a cancel for an
                        // order that never existed and nothing is wrong at the
                        // venue. A replay of one 3.7-day shadow produced ~150 of
                        // those on PM-US alone, and a line that cries wolf 150
                        // times is a line operators stop reading.
                        eprintln!(
                            "[engine] cancel {oid} ({market} on {}) has had no order_ack for \
                             {}s — escalating to a client-id cancel (Kalshi resolves it against \
                             its order list; PM-US cannot and will refuse). A place the venue \
                             rejected looks exactly like this.",
                            venue.as_str(),
                            CANCEL_ACK_GRACE.as_secs()
                        );
                    }
                }
            }
            // Two independent facts, in order of locality: whether the engine's
            // own subscription can be trusted, then whether the recorder says
            // the venue sockets can be. Ungated by `--health` (only by bench)
            // because the FIRST of those is the engine's own business — a run
            // without a health file must still be able to notice, and clear, a
            // disconnect of its own feed.
            _ = feed_iv.tick(), if !cfg.bench => {
                let t = feed_tick(
                    feed_reason.as_ref(),
                    resync_reason(&link, std::time::Instant::now()),
                    cfg.health_file
                        .as_deref()
                        .and_then(|p| feed_stale_reason(p, wall_now(), &required)),
                );
                if t.proven {
                    link = Link::Fresh; // stop re-deriving it
                }
                // Log on any CHANGE of reason, not just healthy<->stale: an
                // engine that is silent must always be able to say why, and
                // "unreadable path" vs "recorder silent" are different bugs.
                if t.log {
                    match &t.reason {
                        Some(why) => eprintln!("[engine] FEED STALE ({why}) — quotes pulled"),
                        None => eprintln!("[engine] feeds healthy — quoting resumes"),
                    }
                }
                if t.sweep {
                    pull_quotes!("FEED STALE");
                }
                feed_reason = t.reason;
            }
            _ = stats_iv.tick(), if !cfg.bench => {
                println!("{}", summary!());
                if let Some(o) = out.as_mut() { o.flush().expect("flush"); }
                // Re-derive the bar: marks are rewritten by arbbot-marks.timer
                // as the book turns over, and holding the startup value would
                // let the engine trade against a stale definition of "good".
                if let Some(tt) = cfg.take_take.as_ref() {
                    let bar = read_bar(&tt.marks_path);
                    let now_bar = bar.tradable();
                    // Edge-triggered, like `feed_reason`: the four hours the
                    // armed session spent firing against a frozen bar produced
                    // not one line saying so, and a line every stats tick is a
                    // line nobody reads. `take_take_bar_apr` in the summary
                    // above renders `None` as null, which is the standing
                    // signal.
                    if now_bar.is_some() != tt_bar.is_some() {
                        eprintln!("[take-take] {}", bar.describe());
                    }
                    tt_bar = now_bar;
                }
            }
        }
    }

    if let Some(o) = out.as_mut() {
        o.flush().expect("final flush");
    }
    // Anything still HELD has run out of chances to be explained: the loop is
    // over, so no `order_ack` is coming for it. Say so once per fill rather than
    // letting the process exit with the count buried in a gauge — and count it,
    // so a bench/replay (which never runs the deadline above) still reports it.
    for (id, u) in std::mem::take(&mut unclaimed_fills) {
        n_unattributed += 1;
        eprintln!(
            "[fill] UNEXPLAINED at exit: {}x on {} {} reported as order {id}, never claimed by \
             an order_ack. Money moved in this account that the engine cannot attribute — \
             RECONCILE BY HAND.",
            u.cum,
            u.venue.as_str(),
            u.market_id
        );
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
    use arb_core::scan::{RelLeg, RelType};

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
            risk: None,
            ledger_path: None, // never write the accounting ledger from a test
            hedge_retry: Some(HedgeRetry {
                interval_s: 0.05,
                max_slip: "0.01".into(),
                alarm_after_s: 0.1,
            }),
            take_take: None,
            armed: false,
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
}

/// The cancel path: a cancel must be addressed with the VENUE's order id, and a
/// reprice must cancel the order it replaces.
///
/// Before 2026-07-28 neither was true. Every per-order cancel carried our own
/// `m…` id, which Kalshi 404s (mapped to success by quirk K4) and PM-US answers
/// <300 for; and a reprice emitted a single place intent carrying `replaces`,
/// whose only effect was FillLedger bookkeeping. The two compose into an engine
/// that believes it holds one quote per market while the venue holds several at
/// stale prices, all live and fillable.
#[cfg(test)]
mod cancel_addressing_tests {
    use super::*;

    /// Action has no Debug (it lives in the exec module and carries venue
    /// request types), so describe it for assertions.
    fn describe(a: &Action) -> String {
        match a {
            Action::Place(p) => format!("place {} @{} as {}", p.market, p.price, p.client_order_id),
            Action::Cancel(c) => format!("cancel {:?}", c.by),
            Action::SweepAndVerify => "sweep".to_string(),
        }
    }

    fn describe_all(v: &[Action]) -> Vec<String> {
        v.iter().map(describe).collect()
    }

    /// The one reprice the armed M3 run made, verbatim from
    /// `data/trader-rs/m3-intents.jsonl` — the line that left two Kalshi asks
    /// resting (5 @ 0.12 and 5 @ 0.13) where the engine believed it held one.
    const LIVE_REPRICE: &str = r#"{"count":5,"old_price":"0.12","order_id":"m1785257819053","place":"KXNOBELPEACE-27-STC","price":"0.13","replaces":"m1785257819045","side":"ask","ts":1785257814.0,"venue":"kalshi"}"#;

    fn json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).expect("test fixture json")
    }

    /// A fixed monotonic origin. `Instant` cannot be constructed from a number,
    /// so the tests take one `now` and add to it — which is also the only way to
    /// exercise the deadline without sleeping.
    fn t0() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn after(t: std::time::Instant, secs: f64) -> std::time::Instant {
        t + std::time::Duration::from_secs_f64(secs)
    }

    /// A cancel goes out addressed with the venue's own id, learned from the ack.
    #[test]
    fn a_cancel_carries_the_venues_order_id_not_ours() {
        let mut oid_venue = HashMap::new();
        oid_venue.insert("m1".to_string(), "66e1c799-507b".to_string());
        let mut parked = HashMap::new();
        let line = json(
            r#"{"cancel":"KXNOBELPEACE-27-STC","order_id":"m1","venue":"kalshi","side":"ask","price":"0.12","ts":1.0}"#,
        );

        let acts = intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t0());
        assert_eq!(
            describe_all(&acts),
            vec![r#"cancel VenueId("66e1c799-507b")"#],
            "the cancel must address the venue's id"
        );
        assert!(parked.is_empty(), "nothing to park once the ack has landed");
        let Action::Cancel(c) = &acts[0] else { panic!("expected a cancel") };
        assert_eq!(
            c.market_slug.as_deref(),
            Some("KXNOBELPEACE-27-STC"),
            "PM-US needs the slug in the body; it rides on every cancel"
        );
    }

    /// No ack yet => the cancel is PARKED, never sent with our id. Sending it
    /// would be the phantom cancel both venues report as success.
    #[test]
    fn a_cancel_with_no_venue_id_yet_is_parked_and_nothing_is_sent() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let line = json(
            r#"{"cancel":"KXNOBELPEACE-27-STC","order_id":"m1","venue":"kalshi","ts":1.0}"#,
        );

        let t = t0();
        let acts = intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        assert!(acts.is_empty(), "nothing may be sent: {:?}", describe_all(&acts));
        let p = parked.get("m1").expect("the cancel must be parked, not dropped");
        assert_eq!(p.venue, Venue::Kalshi);
        assert_eq!(p.market, "KXNOBELPEACE-27-STC");
        assert_eq!(p.since, t);
        assert!(!p.escalated);
    }

    /// A repeated cancel intent for the same order keeps the ORIGINAL park time,
    /// so the escalation deadline cannot be pushed out forever.
    #[test]
    fn re_parking_the_same_cancel_does_not_reset_its_clock() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"K","order_id":"m1","venue":"kalshi","ts":1.0}"#);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, after(t, 8.0));
        assert_eq!(parked["m1"].since, t);
        assert_eq!(cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false).len(), 1);
    }

    /// The escalation: a parked cancel whose ack never arrived is re-addressed by
    /// OUR client id — which Kalshi resolves against its own order list and PM-US
    /// refuses locally. It must not expire early, and it must be offered once.
    #[test]
    fn an_unresolved_cancel_escalates_by_client_id_only_after_the_grace() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"KXTEST","order_id":"m1","venue":"kalshi","ts":1.0}"#);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);

        assert!(
            cancel_work(&parked, &oid_venue, after(t, 14.9), false).is_empty(),
            "must not give up on the ack early"
        );
        assert_eq!(
            cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false),
            vec![CancelWork::Escalate {
                oid: "m1".into(),
                venue: Venue::Kalshi,
                market: "KXTEST".into()
            }]
        );
        // ...and once the caller has actually queued it, never again.
        parked.get_mut("m1").unwrap().escalated = true;
        assert!(
            cancel_work(&parked, &oid_venue, after(t, 1e6), false).is_empty(),
            "one escalation per order — it costs a full paginated account read"
        );
    }

    /// A backward clock step must not freeze the deadline, and a forward one must
    /// not expire everything at once. `Instant` is monotonic, so neither is
    /// reachable — this pins the CHOICE of clock, which a switch back to
    /// `SystemTime` would silently undo.
    #[test]
    fn the_park_deadline_is_immune_to_a_clock_step() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"K","order_id":"m1","venue":"kalshi","ts":1.0}"#);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        // a clock reading from BEFORE the park (what an NTP step back looks like
        // to a wall clock) must not panic and must not escalate
        assert!(cancel_work(&parked, &oid_venue, t - std::time::Duration::from_secs(3600), false)
            .is_empty());
        assert!(!parked.is_empty(), "and the entry survives");
    }

    /// The escalation is capped at ONE per tick: it costs `all_orders()`, a
    /// paginated read of the whole account history, taken inside the venue
    /// executor's one-at-a-time blocking slot — and it competes for the same
    /// budget as the sweep's only proof that the book is clean.
    #[test]
    fn at_most_one_escalation_is_offered_per_tick() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        for oid in ["m1", "m2", "m3"] {
            let line = json(&format!(
                r#"{{"cancel":"K","order_id":"{oid}","venue":"kalshi","ts":1.0}}"#
            ));
            intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        }
        let work = cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false);
        assert_eq!(work.len(), 1, "a burst drains at one per second, not all at once");
        assert_eq!(
            work[0],
            CancelWork::Escalate { oid: "m1".into(), venue: Venue::Kalshi, market: "K".into() },
            "the oldest by id order, deterministically"
        );
    }

    /// While killed, `SweepAndVerify` is already queued and is a strictly better
    /// remedy — it reaches orders we hold no id for at all and it PROVES the
    /// outcome. Escalating on top of it would put paginated reads on the same
    /// budget as the sweep's proof-of-clean, which is how a clean shutdown gets
    /// reported "NOT CLEAN at exit".
    #[test]
    fn a_kill_suppresses_escalation_because_the_sweep_is_the_better_remedy() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"K","order_id":"m1","venue":"kalshi","ts":1.0}"#);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        assert!(cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, true).is_empty());
        assert!(!parked.is_empty(), "still owed, so still counted");
    }

    /// A late ack must still be able to send the REAL cancel, even after the
    /// escalation has been and gone. The concrete case: a place sits behind an
    /// executor backlog, the escalation fires at +15s and PM-US refuses it by
    /// design, then at +20s the place lands and its ack carries the venue id —
    /// the cancel is now perfectly addressable and somebody has to send it.
    #[test]
    fn a_late_ack_still_sends_the_real_cancel_after_an_escalation() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"slug","order_id":"m1","venue":"polymarket_us","ts":1.0}"#);
        intent_actions(&line, Venue::PolymarketUs, true, &oid_venue, &mut parked, t);

        // +15s: the escalation goes out and PM-US refuses it, by design.
        let esc = cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false);
        assert!(matches!(esc[0], CancelWork::Escalate { .. }));
        settle(&mut parked, &esc[0], true);
        assert!(
            parked.contains_key("m1"),
            "an escalation must not retire the entry — the cancel is still owed"
        );

        // +20s: the backlogged place finally lands and its ack carries the id
        oid_venue.insert("m1".to_string(), "BH8H83AY09NG".to_string());
        assert_eq!(
            cancel_work(&parked, &oid_venue, after(t, 20.0), false),
            vec![CancelWork::Send {
                oid: "m1".into(),
                venue: Venue::PolymarketUs,
                market: "slug".into(),
                venue_order_id: "BH8H83AY09NG".into(),
            }],
            "an escalated entry is not a retired entry"
        );
    }

    /// THE rule that keeps the gauge honest: a command that was never QUEUED
    /// changes nothing. `dispatch!` returns false when the executor channel is
    /// full and the command is lost; retiring the park entry on that dropped
    /// `cancels_unresolved` back to 0 while the order went on resting.
    #[test]
    fn a_cancel_that_was_never_queued_stays_owed() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"K","order_id":"m1","venue":"kalshi","ts":1.0}"#);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        oid_venue.insert("m1".to_string(), "v-1".to_string());

        let w = &cancel_work(&parked, &oid_venue, t, false)[0];
        settle(&mut parked, w, false); // channel full: nothing went out
        assert!(parked.contains_key("m1"), "a lost cancel is still owed");
        assert_eq!(
            cancel_work(&parked, &oid_venue, t, false).len(),
            1,
            "and the next tick retries it"
        );
        settle(&mut parked, w, true); // this one really went out
        assert!(parked.is_empty(), "retired only once the command was queued");
    }

    /// Same rule for the escalation: a lost one must not burn the order's single
    /// escalation, or the one venue-side resolution attempt is spent on nothing.
    #[test]
    fn an_escalation_that_was_never_queued_is_not_spent() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = json(r#"{"cancel":"K","order_id":"m1","venue":"kalshi","ts":1.0}"#);
        intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
        let due = t + CANCEL_ACK_GRACE;

        let w = &cancel_work(&parked, &oid_venue, due, false)[0];
        settle(&mut parked, w, false);
        assert!(!parked["m1"].escalated, "a lost escalation did not happen");
        assert_eq!(cancel_work(&parked, &oid_venue, due, false).len(), 1, "retried next tick");
        settle(&mut parked, w, true);
        assert!(parked["m1"].escalated);
        assert!(
            cancel_work(&parked, &oid_venue, due, false).is_empty(),
            "and now it is spent"
        );
    }

    /// The command each item of work actually sends. A `Send` must carry the
    /// venue's id and an `Escalate` must carry ours — swapping them is the whole
    /// original defect.
    #[test]
    fn work_items_address_the_id_space_they_name() {
        let send = CancelWork::Send {
            oid: "m1".into(),
            venue: Venue::Kalshi,
            market: "K".into(),
            venue_order_id: "66e1c799".into(),
        };
        assert_eq!(describe(&send.action()), r#"cancel VenueId("66e1c799")"#);
        let esc =
            CancelWork::Escalate { oid: "m1".into(), venue: Venue::Kalshi, market: "K".into() };
        assert_eq!(describe(&esc.action()), r#"cancel ClientId("m1")"#);
    }

    /// Every parked cancel whose venue id is known is retried — a `Send` is one
    /// targeted DELETE for a cancel we already owe, so it is not rate-capped the
    /// way an escalation is.
    #[test]
    fn resolvable_cancels_are_all_retried_not_capped() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        for oid in ["m1", "m2"] {
            let line = json(&format!(
                r#"{{"cancel":"K","order_id":"{oid}","venue":"kalshi","ts":1.0}}"#
            ));
            intent_actions(&line, Venue::Kalshi, true, &oid_venue, &mut parked, t);
            oid_venue.insert(oid.to_string(), format!("v-{oid}"));
        }
        let work = cancel_work(&parked, &oid_venue, t, false);
        assert_eq!(work.len(), 2, "{work:?}");
        assert!(matches!(work[0], CancelWork::Send { .. }));
    }

    /// THE reprice fix: an amend is a cancel AND a place, cancel first.
    #[test]
    fn a_reprice_cancels_the_replaced_order_before_placing_the_new_one() {
        let mut oid_venue = HashMap::new();
        oid_venue.insert("m1785257819045".to_string(), "d56aa591-4b72".to_string());
        let mut parked = HashMap::new();

        let acts = intent_actions(
            &json(LIVE_REPRICE),
            Venue::Kalshi,
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(
            describe_all(&acts),
            vec![
                r#"cancel VenueId("d56aa591-4b72")"#,
                "place KXNOBELPEACE-27-STC @0.13 as m1785257819053",
            ],
            "the replaced order must be cancelled, and cancelled FIRST"
        );
    }

    /// ...and the cancel names the order being REPLACED, never the new one.
    #[test]
    fn the_reprice_cancel_names_the_old_order_not_the_new_one() {
        let mut oid_venue = HashMap::new();
        oid_venue.insert("m1785257819045".to_string(), "old-venue-id".to_string());
        oid_venue.insert("m1785257819053".to_string(), "new-venue-id".to_string());
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &json(LIVE_REPRICE),
            Venue::Kalshi,
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        let Action::Cancel(c) = &acts[0] else { panic!("expected the cancel first") };
        assert_eq!(c.by, CancelBy::VenueId("old-venue-id".into()));
    }

    /// A reprice whose replaced order has no ack yet still PLACES — the quoter
    /// has already moved its resting state and cannot be told otherwise, so
    /// withholding the place would make the market go quietly dark. The cancel
    /// is parked and counted instead of being sent to a phantom.
    #[test]
    fn a_reprice_with_an_unacked_old_order_places_and_parks_the_cancel() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &json(LIVE_REPRICE),
            Venue::Kalshi,
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(
            describe_all(&acts),
            vec!["place KXNOBELPEACE-27-STC @0.13 as m1785257819053"]
        );
        assert!(parked.contains_key("m1785257819045"), "the cancel is owed, not forgotten");
    }

    /// A plain place is still exactly one command, and a hedge is still an IOC.
    #[test]
    fn a_place_without_replaces_emits_one_command() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &json(
                r#"{"place":"KXTEST","order_id":"m9","venue":"kalshi","side":"bid","price":"0.31","count":25,"ts":1.0}"#,
            ),
            Venue::Kalshi,
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(acts.len(), 1);
        let Action::Place(p) = &acts[0] else { panic!("expected a place") };
        assert_eq!(p.qty, 25);
        assert!(p.post_only, "a maker quote rests");
        assert!(matches!(p.tif, Tif::Gtc));

        let hedge = intent_actions(
            &json(
                r#"{"place":"KXTEST","order_id":"h1","venue":"kalshi","side":"ask","price":"0.30","count":25,"tag":"hedge","taker":true,"ts":1.0}"#,
            ),
            Venue::Kalshi,
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        let Action::Place(p) = &hedge[0] else { panic!("expected a place") };
        assert!(!p.post_only, "a hedge crosses");
        assert!(matches!(p.tif, Tif::Ioc));
    }

    /// An UNARMED engine never receives an ack (the executor drops the command
    /// before any venue call), so parking would accumulate forever and prove
    /// nothing. It addresses the cancel by our client id — an honest statement
    /// of all it knows — and the inert sink drops it.
    #[test]
    fn a_dry_run_addresses_a_cancel_by_our_client_id_and_never_parks() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &json(r#"{"cancel":"KXTEST","order_id":"m1","venue":"kalshi","ts":1.0}"#),
            Venue::Kalshi,
            false,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(describe_all(&acts), vec![r#"cancel ClientId("m1")"#]);
        assert!(parked.is_empty(), "an unarmed engine has nothing to wait for");
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

    /// The keys a registry over `venues` requires, through the real derivation.
    fn required(venues: &[Venue]) -> Vec<String> {
        let mut by_market: HashMap<(Venue, String), Vec<usize>> = HashMap::new();
        for (i, v) in venues.iter().enumerate() {
            by_market.insert((*v, format!("M{i}")), vec![0]);
        }
        required_feeds(&by_market)
    }

    /// What the live drop-in quotes: Kalshi and PM-US.
    const QUOTED: [Venue; 2] = [Venue::Kalshi, Venue::PolymarketUs];

    #[test]
    fn healthy_feeds_do_not_pull() {
        let (_d, p) = health_file(&[&line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW, &required(&QUOTED)), None);
    }

    /// polymarket (INTL) is not a critical feed — the money path is Kalshi and
    /// PM-US, so intl staleness must not pull quotes. The fixture always sets
    /// polymarket-ws stale.
    ///
    /// It stays excluded even when the registry QUOTES it, which 6 of the 40
    /// live relationships do: see `DATA_ONLY_VENUES` for the reason and for the
    /// exposure that exclusion leaves open.
    #[test]
    fn a_non_critical_feed_going_stale_does_not_pull() {
        let (_d, p) = health_file(&[&line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW, &required(&QUOTED)), None);
        let with_intl = required(&[Venue::Kalshi, Venue::PolymarketUs, Venue::Polymarket]);
        assert_eq!(with_intl, vec!["kalshi-ws", "polymarket_us-ws"], "INTL is data-only here");
        assert_eq!(feed_stale_reason(&p, NOW, &with_intl), None);
    }

    #[test]
    fn either_critical_feed_pulls_all_quotes() {
        for (k, pm, want) in [(true, false, "kalshi-ws"), (false, true, "polymarket_us-ws")] {
            let (_d, p) = health_file(&[&line(NOW - 1.0, k, pm)]);
            let why = feed_stale_reason(&p, NOW, &required(&QUOTED)).expect("must pull");
            assert!(why.contains(want), "{why}");
        }
    }

    /// C4(a). `data/health.jsonl` names every feed it is WATCHING in `stale`,
    /// and the engine used to read a name that was simply not there as healthy
    /// (`unwrap_or(false)`). So whichever venue the recorder happened not to
    /// report was the one venue the engine could never pull for — in an
    /// otherwise fail-closed function. Not hypothetical: 37,639 health lines on
    /// 2026-07-20 carried `kalshi-rest` and no `kalshi-ws` at all, and every one
    /// of them read as Kalshi-healthy on no evidence whatsoever.
    #[test]
    fn an_absent_staleness_key_reads_as_stale_not_healthy() {
        let l = format!(
            r#"{{"ts":{},"stale":{{"polymarket_us-ws":false,"polymarket-ws":false}}}}"#,
            NOW - 1.0
        );
        let (_d, p) = health_file(&[&l]);
        let why =
            feed_stale_reason(&p, NOW, &required(&QUOTED)).expect("an unreported feed must pull");
        assert!(why.contains("kalshi-ws"), "{why}");
        assert!(why.contains("unreported"), "{why}");
        // ...and a `stale` object that is missing entirely is not a clean bill
        // of health either.
        let (_d2, p2) = health_file(&[&format!(r#"{{"ts":{}}}"#, NOW - 1.0)]);
        assert!(feed_stale_reason(&p2, NOW, &required(&QUOTED)).is_some());
    }

    /// ...which is only safe because the required set is DERIVED: a venue this
    /// registry does not quote must be able to be absent from the health file
    /// forever without pulling a single quote. A hardcoded pair cannot express
    /// that, and paired with the absent-key rule above it would pull the engine
    /// silent permanently.
    #[test]
    fn a_venue_we_do_not_quote_is_not_required() {
        assert_eq!(required(&QUOTED), vec!["kalshi-ws", "polymarket_us-ws"]);
        // A Kalshi-only registry (`--rel-prefix` narrowing does exactly this)
        // must not need a PM-US entry it has no use for.
        assert_eq!(required(&[Venue::Kalshi]), vec!["kalshi-ws"]);
        let l = format!(r#"{{"ts":{},"stale":{{"kalshi-ws":false}}}}"#, NOW - 1.0);
        let (_d, p) = health_file(&[&l]);
        assert_eq!(
            feed_stale_reason(&p, NOW, &required(&[Venue::Kalshi])),
            None,
            "an absent PM-US entry cannot pull an engine that quotes no PM-US market"
        );
        // ...and the same file DOES pull once PM-US is quoted.
        let why = feed_stale_reason(&p, NOW, &required(&QUOTED)).expect("must pull");
        assert!(why.contains("polymarket_us-ws"), "{why}");
    }

    /// The health writer going quiet means the recorder is down — worse than
    /// any single feed, and the flags in the last line are stale evidence.
    #[test]
    fn a_silent_recorder_is_stale_even_when_the_last_line_looked_healthy() {
        let (_d, p) = health_file(&[&line(NOW - 120.0, false, false)]);
        let why = feed_stale_reason(&p, NOW, &required(&QUOTED)).expect("must pull");
        assert!(why.contains("recorder silent"), "{why}");
    }

    /// Only the LAST line counts; an old healthy line must not rescue a new
    /// stale one.
    #[test]
    fn only_the_last_line_is_read() {
        let (_d, p) = health_file(&[&line(NOW - 5.0, false, false), &line(NOW - 1.0, true, false)]);
        assert!(
            feed_stale_reason(&p, NOW, &required(&QUOTED)).is_some(),
            "the newest line is stale"
        );
    }

    /// FAIL-CLOSED: no file, no readable line, or garbage all pull the quotes.
    /// Python left the state unchanged here, which would quote forever on a
    /// feed it could not see.
    #[test]
    fn an_unreadable_health_file_pulls_quotes() {
        let req = required(&QUOTED);
        assert!(feed_stale_reason("/nonexistent/health.jsonl", NOW, &req).is_some());
        let (_d, p) = health_file(&["not json at all"]);
        assert!(feed_stale_reason(&p, NOW, &req).is_some());
        let (_d2, p2) = health_file(&[]);
        assert!(feed_stale_reason(&p2, NOW, &req).is_some(), "an empty file proves nothing");
    }

    /// A line with no `ts` is treated as infinitely old, not as ts=now.
    #[test]
    fn a_line_without_a_timestamp_is_stale() {
        let (_d, p) = health_file(&[r#"{"stale":{"kalshi-ws":false,"polymarket_us-ws":false}}"#]);
        assert!(feed_stale_reason(&p, NOW, &required(&QUOTED)).is_some());
    }

    /// The tail window can start mid-codepoint; that must not panic or hide a
    /// healthy line.
    #[test]
    fn a_large_file_reads_only_its_tail() {
        let pad = format!(r#"{{"ts":1,"note":"{}"}}"#, "é".repeat(3000));
        let (_d, p) = health_file(&[&pad, &line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW, &required(&QUOTED)), None);
    }

    /// C4(b), the policy half. A disconnect must pull, and a RECONNECT is not
    /// evidence that anything was repaired — the welcome snapshot burst is.
    #[test]
    fn a_reconnect_alone_does_not_prove_the_books_are_current() {
        let t = std::time::Instant::now();
        let hour = std::time::Duration::from_secs(3600);
        let why = resync_reason(&Link::Down, t).expect("a disconnect must pull");
        assert!(why.contains("disconnected"), "{why}");
        // Reconnected, welcome burst never seen: still pulled, however long we
        // wait. A socket that accepts and then says nothing is not a healed
        // feed.
        let bare = Link::Resyncing { since: t, snapshots: 0 };
        assert!(resync_reason(&bare, t + hour).is_some());
        // Burst arriving, but not yet given time to finish.
        let mid = Link::Resyncing { since: t, snapshots: 1 };
        assert!(resync_reason(&mid, t).is_some());
        assert!(
            resync_reason(&mid, t + RESYNC_SETTLE - std::time::Duration::from_millis(1)).is_some()
        );
        // Both pieces of evidence, and only then.
        assert_eq!(resync_reason(&mid, t + RESYNC_SETTLE), None);
        assert_eq!(resync_reason(&Link::Fresh, t), None);
    }
}

/// The pull WIRING, driven through the real `run()` loop: what the engine
/// actually does to the executors when its feed goes away or its marks go
/// stale. The audit found the suite covered the pure half of the money path and
/// almost none of the concurrent half; these four are that half.
#[cfg(test)]
mod feed_wiring_tests {
    use super::*;
    use arb_core::scan::{RelLeg, RelType};

    /// The two shapes `run` is handed, named so the signatures below stay
    /// readable.
    type ByMarket = HashMap<(Venue, String), Vec<usize>>;
    type Executors = (HashMap<Venue, mpsc::Sender<ExecCmd>>, Vec<(Venue, mpsc::Receiver<ExecCmd>)>);

    /// One cross-venue relationship whose resolve date is in the table, so
    /// take-take can price it.
    fn fixture() -> (Vec<Quoter>, ByMarket) {
        let rel = Rel {
            id: "xvus-nobel-peace-26-b4b".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        };
        let by_market = HashMap::from([
            ((Venue::Kalshi, "K".to_string()), vec![0usize]),
            ((Venue::PolymarketUs, "P".to_string()), vec![0usize]),
        ]);
        (vec![Quoter::new(rel)], by_market)
    }

    fn cfg(
        out: &std::path::Path,
        health_file: Option<String>,
        take_take: Option<TakeTake>,
    ) -> RunCfg {
        RunCfg {
            out_path: Some(out.to_string_lossy().into_owned()),
            kill_file: "/nonexistent/KILL".into(),
            // No stats tick inside a test: it would re-derive the bar off the
            // wall clock and print into the harness.
            stats_every_s: 86_400,
            bench: false,
            wal_path: None,
            health_file,
            risk: None,
            ledger_path: None,
            hedge_retry: None,
            take_take,
            armed: false,
        }
    }

    fn stats() -> Arc<ExecStats> {
        use std::sync::atomic::AtomicU64;
        Arc::new(ExecStats {
            hop: Hist::new(),
            placed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        })
    }

    /// Executor channels for the two venues an armed engine holds.
    fn executors() -> Executors {
        let mut txs = HashMap::new();
        let mut rxs = Vec::new();
        for v in [Venue::Kalshi, Venue::PolymarketUs] {
            let (tx, rx) = mpsc::channel(64);
            txs.insert(v, tx);
            rxs.push((v, rx));
        }
        (txs, rxs)
    }

    /// How many sweep-and-verify commands each venue's executor was sent.
    fn sweeps(rxs: &mut [(Venue, mpsc::Receiver<ExecCmd>)]) -> Vec<(Venue, usize)> {
        let mut out = Vec::new();
        for (v, rx) in rxs.iter_mut() {
            let mut n = 0;
            while let Ok(c) = rx.try_recv() {
                if matches!(c.action, Action::SweepAndVerify) {
                    n += 1;
                }
            }
            out.push((*v, n));
        }
        out.sort_by_key(|(v, _)| v.as_str());
        out
    }

    fn snapshot(venue: &str, market: &str, bid: &str, ask: &str, ts: f64) -> String {
        json!({"kind": "snapshot", "venue": venue, "market_id": market,
               "bids": [{"price": bid, "size": "50"}],
               "asks": [{"price": ask, "size": "50"}],
               "seq": 1, "ts_local_ns": (ts * 1e9) as i64})
        .to_string()
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("arb-trader-b4b-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join(name);
        let _ = std::fs::remove_file(&p);
        p
    }

    /// `YYYY-MM-DDTHH:MM:SSZ` — the one shape `bar_from_marks` accepts.
    fn iso_z(t: f64) -> String {
        let s = (t as i64).rem_euclid(86_400);
        format!(
            "{}T{:02}:{:02}:{:02}Z",
            crate::taketake::today_iso(t),
            s / 3600,
            (s % 3600) / 60,
            s % 60
        )
    }

    /// A marks file that yields a real blended bar (~4.7%/yr), generated at `t`.
    fn marks_at(t: f64) -> String {
        format!(
            r#"{{"generated_at":"{}","positions":[{{"cost_usd":100.0,"locked_profit_usd":2.0,"resolves_by":"2026-12-31"}}]}}"#,
            iso_z(t)
        )
    }

    fn take_take(marks_path: &std::path::Path) -> TakeTake {
        TakeTake {
            max_ct_per_rel: 50,
            max_clip: 5,
            marks_path: marks_path.to_string_lossy().into_owned(),
            detect_only: false,
            cooldown_s: 0.0,
        }
    }

    /// Feed `lines`, then close the feed and let `run` return its summary. No
    /// timer arm can fire: `biased` polls the feed first and it is always ready
    /// (message, then closed), which is what makes this deterministic.
    #[allow(clippy::type_complexity)]
    async fn drive(
        cfg: RunCfg,
        out: &std::path::Path,
        lines: &[String],
    ) -> (serde_json::Value, String, Vec<(Venue, usize)>) {
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        for l in lines {
            tx.try_send(FeedMsg { line: l.clone(), t_read: std::time::Instant::now() })
                .expect("test channel");
        }
        drop(tx);
        let (txs, mut rxs) = executors();
        let summary = run(quoters, by_market, rx, txs, stats(), cfg).await;
        let intents = std::fs::read_to_string(out).unwrap_or_default();
        (summary, intents, sweeps(&mut rxs))
    }

    /// C4(b) + C4(c). The armed engine was dropped by the recorder repeatedly on
    /// 2026-07-28 while `feed_pulled` stayed false the whole session. Now a
    /// disconnect pulls — and the pull SWEEPS, because `cancel_all` reaches only
    /// orders the engine still holds ids for and verifies none of them.
    #[tokio::test]
    async fn a_feed_disconnect_pulls_quotes_and_sweeps_every_armed_venue() {
        let out = scratch("disconnect-intents.jsonl");
        let feed = [
            json!({"kind": crate::feed::FEED_UP}).to_string(),
            snapshot("kalshi", "K", "0.03", "0.04", 1_785_211_200.0),
            json!({"kind": crate::feed::FEED_DOWN, "note": "subscriber dropped"}).to_string(),
        ];
        let (summary, _intents, swept) = drive(cfg(&out, None, None), &out, &feed).await;
        assert_eq!(summary["feed_pulled"], serde_json::json!(true), "{summary}");
        assert_eq!(
            swept,
            vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)],
            "the pull must PROVE both books empty, like KILL does"
        );
    }

    /// ...and a reconnect on its own does not resume quoting: the engine is
    /// still pulled after FEED_UP, because the welcome burst has not landed. A
    /// flapping subscriber therefore re-proves the book ONCE per pull episode,
    /// not once per flap — nothing can start resting while quotes are pulled,
    /// and a sweep is a venue round trip against the order path's rate budget.
    #[tokio::test]
    async fn a_reconnect_does_not_resume_quoting_and_a_flap_sweeps_once() {
        let out = scratch("reconnect-intents.jsonl");
        let up = json!({"kind": crate::feed::FEED_UP}).to_string();
        let down = json!({"kind": crate::feed::FEED_DOWN, "note": "dropped"}).to_string();
        let feed = [up.clone(), down.clone(), up.clone(), down, up];
        let (summary, _i, swept) = drive(cfg(&out, None, None), &out, &feed).await;
        assert_eq!(
            summary["feed_pulled"],
            serde_json::json!(true),
            "a reconnect is when the books become repairABLE, not repaired: {summary}"
        );
        assert_eq!(swept, vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)]);
    }

    /// C4(c) on the health-file path. The tick's decision is a pure function so
    /// this needs no runtime; `pull_quotes!` — the thing it asks for — is proven
    /// to reach both executors by the disconnect test above.
    #[test]
    fn a_stale_health_file_pulls_and_sweeps_only_on_the_way_in() {
        // quoting -> pulled: cancel AND prove the books empty
        let t = feed_tick(None, None, Some("kalshi-ws unreported by the recorder".into()));
        assert!(t.sweep, "the feed-stale pull must sweep, not just stop quoting");
        assert!(t.log);
        assert_eq!(t.reason.as_deref(), Some("kalshi-ws unreported by the recorder"));
        // pulled -> pulled for a NEW reason: say so; nothing left to cancel
        let was = "kalshi-ws unreported by the recorder".to_string();
        let t = feed_tick(Some(&was), None, Some("recorder silent for 90s".into()));
        assert!(t.log && !t.sweep);
        // pulled -> pulled, same reason: silence, and no sweep per tick
        let t = feed_tick(Some(&was), None, Some(was.clone()));
        assert!(!t.log && !t.sweep);
        // ...and healthy again
        let t = feed_tick(Some(&was), None, None);
        assert!(t.log && !t.sweep && t.reason.is_none() && t.proven);
    }

    /// The engine's own subscription is the more local fact and outranks the
    /// recorder's report: the recorder can be perfectly healthy while WE are the
    /// ones not connected to it, which is exactly what happened all day on
    /// 2026-07-28.
    #[test]
    fn our_own_subscription_outranks_the_recorders_report() {
        let t = feed_tick(None, Some("feed disconnected".into()), None);
        assert_eq!(t.reason.as_deref(), Some("feed disconnected"));
        assert!(t.sweep, "a disconnect the tick discovers must sweep too");
        assert!(!t.proven, "a disconnect is not evidence of anything");
        // ...and once the subscription is proven, the health file decides.
        let t = feed_tick(None, None, Some("kalshi-ws stale".into()));
        assert!(t.proven);
        assert_eq!(t.reason.as_deref(), Some("kalshi-ws stale"));
    }

    /// C9's staleness half, wired. `marks.json` froze at 12:46:12 on 2026-07-28
    /// and the armed session fired take-take for four hours against that frozen
    /// bar. A stale marks file now yields NO bar, and no bar means no take-take.
    #[tokio::test]
    async fn a_stale_marks_file_refuses_the_take_take_bar() {
        // Same crossing, same books, same everything but the marks timestamp.
        let ts = 1_785_211_200.0; // 2026-07-28T00:00:00Z, ~0.2yr to resolution
        let feed = [
            snapshot("kalshi", "K", "0.02", "0.03", ts),
            snapshot("polymarket_us", "P", "0.08", "0.09", ts),
        ];

        let fresh = scratch("marks-fresh.json");
        std::fs::write(&fresh, marks_at(wall_now())).unwrap();
        let out_f = scratch("marks-fresh-intents.jsonl");
        let (sum_f, int_f, _) =
            drive(cfg(&out_f, None, Some(take_take(&fresh))), &out_f, &feed).await;
        assert!(
            int_f.contains(r#""tag":"take-take""#),
            "the control case must FIRE, or the test proves nothing: {int_f}"
        );
        assert!(sum_f["take_take_bar_apr"].is_f64(), "{sum_f}");

        let stale = scratch("marks-stale.json");
        std::fs::write(&stale, marks_at(wall_now() - 4.0 * 3600.0)).unwrap();
        let out_s = scratch("marks-stale-intents.jsonl");
        let (sum_s, int_s, _) =
            drive(cfg(&out_s, None, Some(take_take(&stale))), &out_s, &feed).await;
        assert!(
            !int_s.contains(r#""tag":"take-take""#),
            "a four-hour-old marks file must refuse the bar: {int_s}"
        );
        assert_eq!(sum_s["take_take_bar_apr"], serde_json::Value::Null, "{sum_s}");
        assert_eq!(sum_s["take_take_found"], serde_json::json!(0), "{sum_s}");
    }

    /// C5's logging half. The detector has refused crossed books since 4542e5f,
    /// but the engine dropped the reason on the floor — so six hours of live
    /// book corruption on KXRATECUT-26DEC31 logged nothing at all. The reason
    /// string matches the maker path's, so one grep covers both.
    #[tokio::test]
    async fn a_crossed_book_take_take_skip_is_logged() {
        let ts = 1_785_211_200.0;
        let marks = scratch("marks-crossed.json");
        std::fs::write(&marks, marks_at(wall_now())).unwrap();
        let out = scratch("crossed-intents.jsonl");
        // Kalshi offering 0.03 under its own 0.09 bid: impossible on a live
        // venue, so OUR book is corrupt.
        let feed = [
            snapshot("polymarket_us", "P", "0.08", "0.09", ts),
            snapshot("kalshi", "K", "0.09", "0.03", ts),
        ];
        let (summary, intents, _) =
            drive(cfg(&out, None, Some(take_take(&marks))), &out, &feed).await;
        assert!(
            intents.contains("crossed book kalshi take-take xvus-nobel-peace-26-b4b"),
            "the take-take skip must be greppable: {intents}"
        );
        // ...and the maker path's own crossed-book skip shares the prefix, so
        // one `grep "crossed book"` covers both paths.
        assert!(intents.contains("crossed book kalshi K bid"), "{intents}");
        assert_eq!(summary["take_take_found"], serde_json::json!(0), "{summary}");
        assert!(!intents.contains(r#""tag":"take-take""#), "{intents}");
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
            strategy: "maker-hedge",
        };
        let hedge = HedgeOrder {
            maker_order_id: "m1".into(),
            chain_id: "h1".into(),
            market_id: "demo-slug".into(),
            venue: "polymarket_us",
            side: "ask",
            price: "0.40".into(),
            qty: 5,
            cum_filled: 0,
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
        assert_eq!(first["strategy"], "maker-hedge");
        assert_eq!(
            first["fees_pending"], true,
            "the engine does not know venue fees; the record must not pretend otherwise"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A take-take basket must NOT book as maker-hedge. It reuses the same
    /// fill -> hedge pipeline, so before this was fixed every crossing landed
    /// in the ledger as `strategy: maker-hedge` with leg 1 tagged `maker`,
    /// which is two lies in the accounting record: P&L could not be attributed
    /// between the strategies, and the same trade had a different name
    /// depending on whether Python or Rust made it (auto_take_take.py writes
    /// `take-take`).
    #[test]
    fn a_take_take_basket_books_as_take_take_with_a_taker_leg() {
        let dir = std::env::temp_dir().join(format!("arb-book-tt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.jsonl");
        let p = path.to_str().unwrap();

        let maker = MakerOrder {
            rel_id: "xvus-nobel-peace-26-donaldtrump".into(),
            class: "cross-venue-equivalent",
            venue: "polymarket_us".into(),
            market_id: "tac-nobel-peace-2026-10-09-dontru".into(),
            side: "ask".into(),
            price: "0.0800".into(),
            strategy: "take-take",
        };
        let hedge = HedgeOrder {
            maker_order_id: "t1".into(),
            chain_id: "h1".into(),
            market_id: "KXNOBELPEACE-26-DJT".into(),
            venue: "kalshi",
            side: "bid",
            price: "0.0400".into(),
            qty: 5,
            cum_filled: 0,
        };
        book_basket(p, &maker, &hedge, 5, 1_700_000_000.0);

        let rec: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(p).unwrap().lines().next().unwrap())
                .unwrap();
        assert_eq!(rec["strategy"], "take-take");
        assert_eq!(rec["title"], "xvus-nobel-peace-26-donaldtrump (rust take-take)");
        assert_eq!(rec["legs"][0]["role"], "taker", "leg 1 crossed the book, it did not rest");
        assert_eq!(rec["legs"][1]["role"], "taker", "leg 2 is the crossing hedge");
        // and it must still seed exposure like any other basket
        let open = crate::ledger::open_exposure(crate::ledger::read(p).unwrap());
        assert_eq!(open.get("xvus-nobel-peace-26-donaldtrump"), Some(&5.0));
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
            &mut cx, pol, owed, filled, anchor, "bid", Some(touch), t, after(t, 100.0), false,
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
            hedge_plan(&mut cx, &p, 5, 0, "0.40", "bid", Some("0.40"), t, after(t, 4.9), false),
            HedgePlan::Hold,
            "interval_s gates PLACEMENTS"
        );
        // no level on the side we take is a WAIT, not a silent skip
        assert_eq!(
            hedge_plan(&mut cx, &p, 5, 0, "0.40", "bid", None, t, after(t, 100.0), false),
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
                    "bid",
                    Some(touch),
                    t,
                    after(t, 100.0 + i as f64 * 10.0),
                    false,
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
        assert!(first_attempt_acceptable(&mut cx, Some(&p), "bid", "0.39", "0.40"));
        assert!(
            !first_attempt_acceptable(&mut cx, Some(&p), "bid", "0.36", "0.40"),
            "a 4c move against us between place and fill is not a hedge, it is a loss"
        );
        // buying from an ask: worse is paying more
        assert!(first_attempt_acceptable(&mut cx, Some(&p), "ask", "0.41", "0.40"));
        assert!(!first_attempt_acceptable(&mut cx, Some(&p), "ask", "0.45", "0.40"));
        // ...and with NO retry policy there is nothing to carry a refusal
        // forward, so bench/replay fires unconditionally and stays byte-exact.
        assert!(
            first_attempt_acceptable(&mut cx, None, "bid", "0.36", "0.40"),
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
            hedge_plan(&mut cx, &p, 5, 0, "0.40", "ask", Some("0.41"), t, after(t, 100.0), false),
            HedgePlan::Retry { qty: 5, price: "0.41".into() }
        );
        assert_eq!(
            hedge_plan(&mut cx, &p, 5, 0, "0.40", "ask", Some("0.42"), t, after(t, 100.0), false),
            HedgePlan::Wait
        );
        // selling: worse is receiving less
        assert_eq!(plan(&p, 5, 0, "0.40", "0.39"), HedgePlan::Retry {
            qty: 5,
            price: "0.39".into()
        });
        assert_eq!(plan(&p, 5, 0, "0.40", "0.38"), HedgePlan::Wait);
        assert_eq!(taking_side("bid"), "ask", "taking a bid SELLS");
        assert_eq!(taking_side("ask"), "bid", "taking an ask BUYS");
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
            hedge_plan(&mut cx, &p, 5, 0, "0.40", "bid", Some("0.40"), t, after(t, 10.0), false),
            HedgePlan::Retry { qty: 5, price: "0.40".into() },
            "with nothing outstanding this retry is due and priced fine"
        );
        assert_eq!(
            hedge_plan(&mut cx, &p, 5, 0, "0.40", "bid", Some("0.40"), t, after(t, 10.0), true),
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
                    "bid",
                    Some("0.40"),
                    t,
                    after(t, due - 0.1),
                    true
                ),
                HedgePlan::HoldForAck,
                "inside the grace the ack may still land (alarm {alarm_after_s})"
            );
            assert_eq!(
                hedge_plan(&mut cx, &p, 5, 0, "0.40", "bid", Some("0.40"), t, after(t, due), true),
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
            "bid",
            Some("0.40"),
            fill_at,
            after(fill_at, 30.0),
            false,
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
                "bid",
                Some("0.40"),
                after(fill_at, 3600.0),
                fill_at,
                false
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
            hedge_anchor(&rel, "P", "ask", &books, 1.0).is_none(),
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
        let a = hedge_anchor(&rel, "P", "ask", &books, 1.0).expect("a sane book anchors");
        assert_eq!(a.price, "0.1820");
        assert_eq!(a.side, "ask");
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
            side: "bid".into(),
            price: "0.31".into(),
            strategy: "maker-hedge",
        };
        let hedge = HedgeOrder {
            maker_order_id: "m1".into(),
            chain_id: "h1".into(),
            market_id: "SYNTH-P-YES".into(),
            venue: "polymarket_us",
            side: "ask",
            price: "0.40".into(),
            qty: 10,
            cum_filled: 0,
        };
        let f1 = hedge_credit(4, 10, 0, Some((10, 0)));
        let f2 = hedge_credit(10, 10, 4, Some((10, 4)));
        book_basket(p, &maker, &hedge, f1.book, 1_700_000_000.0);
        book_basket(p, &maker, &hedge, f2.book, 1_700_000_001.0);

        let open = crate::ledger::open_exposure(crate::ledger::read(p).unwrap());
        assert_eq!(
            open.get("synth-fill-rel"),
            Some(&10.0),
            "a 10-lot hedge that filled 4 then 6 is 10 contracts of exposure, not 4"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
