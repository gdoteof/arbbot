//! Single-owner engine task: books + quoters + decision policy live here and
//! nowhere else (no locks). Consumes the feed channel; emits canonical intent
//! lines (identical bytes to arb-intent / scripts/intent_replay.py) and
//! routes effect commands to per-venue executors. Time-based behavior runs on
//! deadlines (tokio intervals) in the same select loop — kill-switch watch
//! and stats — never on per-event syscalls.
//!
//! Split by concern, each submodule carrying the tests for its own invariants:
//!
//!   [`hedge`]        an obligation's life from the maker fill that mints it to
//!                    the attempt that discharges it: `PendingHedge` and its
//!                    I1-I5 contract, the retry policy, the anchor.
//!   [`fill`]         what one fill frame MEANS — `Engine::attribute_fill` and
//!                    the two callers that must never disagree about it — and
//!                    the ledger record a discharged obligation books.
//!   [`cancel`]       addressing a cancel at a venue that accepts only its own
//!                    order id, and parking one that cannot be addressed yet.
//!   [`feed_health`]  whether the books are current enough to quote off at all.
//!
//! This module keeps the state those four share (`Engine`), the feed arm that
//! drives them, and `run()` itself.

mod cancel;
mod feed_health;
mod fill;
mod hedge;

use crate::exec::{Action, ExecCmd, ExecStats};
use crate::feed::FeedMsg;
use crate::hist::Hist;
use crate::wal::Wal;
use arb_core::book::{ApplyError, BookBuilder};
use arb_core::clock::now_s as wall_now;
use arb_core::fees::FeeSchedule;
use arb_core::fill::{dropped_unconsumed, FillLedger};
use arb_core::intent::{self, Intent, Tag};
use arb_core::model::{BookSide, Level, Venue};
use arb_core::quoter::{Quoter, RiskGate};
use arb_core::scan::{Cx, Rel};
use cancel::{intent_actions, ParkedCancel};
use feed_health::{required_feeds, resync_reason, Link};
use fill::{MakerOrder, UnclaimedFill};
use hedge::{hedge_anchor, HedgeOrder, PendingHedge};
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
    /// Contracts this unit's PREVIOUS run owed a hedge for and never booked,
    /// counted at startup by `orphan::undischarged`. Carried only to be
    /// reported: the standing gauge is the half of that census a monitor sees,
    /// and it is what makes `hedges_pending: 0` honest after a restart that
    /// forgot an obligation (2026-07-29 01:34). The engine never acts on it —
    /// `orphan` documents why the second hedger would be a double hedge.
    pub hedges_undischarged: u64,
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

/// The take-take bar as the marks file currently supports it.
///
/// An unreadable file is "no file" — a cold start, which `bar_from_marks`
/// answers with `DEFAULT_BAR_APR`. A file that is PRESENT and stale or corrupt
/// is a refusal, and the two must not share an answer.
fn read_bar(path: &str) -> crate::taketake::Bar {
    crate::taketake::bar_from_marks(&std::fs::read_to_string(path).unwrap_or_default(), wall_now())
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

/// The quoter index a book event is looked up in: (venue, market) -> the
/// quoters that quote it.
type ByMarket = HashMap<(Venue, String), Vec<usize>>;

/// Everything the engine knows: the books, the id counters, the order and fill
/// maps, the outstanding hedge obligations, the parked cancels, the gauges.
///
/// This used to be ~30 free-standing `let mut` locals at the top of `run()`,
/// and that is exactly why this file carried five `macro_rules!` where it
/// should have carried five methods. A closure cannot borrow a set of locals
/// mutably, so every piece of logic two arms shared had to be textually
/// substituted back into the one function that owned them — and nothing that is
/// substituted can be called from a test. The hedge arm's own comment records
/// the price: it "had no seam that made it testable at all, which is why all
/// four of its defects survived".
///
/// `quoters` and `by_market` are deliberately NOT fields. They are passed
/// alongside `&mut self` to the arms that need both, because the quote loop
/// holds `&quoters[qi].rel` across a call that drains that quoter's intents —
/// legal only while the quoters and the engine state live in different objects.
/// Making them fields would force a clone of a `Rel` (or of the whole quoter
/// vector) on the hottest path in the process.
struct Engine {
    cfg: RunCfg,
    exec_txs: HashMap<Venue, mpsc::Sender<ExecCmd>>,
    exec_stats: Arc<ExecStats>,
    cx: Cx,
    fees: FeeSchedule,
    books: BookBuilder,
    digest: Sha256,
    decision: Hist,
    /// Intents the current decision produced, awaiting `drain_intents`.
    intents: Vec<Intent>,
    out: Option<std::io::BufWriter<std::fs::File>>,
    wal: Option<Wal>,
    t_start: std::time::Instant,
    /// The health-file keys we require evidence for — the venues we quote.
    required: Vec<String>,
    n_ev: u64,
    n_book: u64,
    n_int: u64,
    /// Crossings the detector found above the bar. In detect_only these are
    /// opportunities we DECLINED to take, which is the number worth watching
    /// before arming the taker path.
    n_tt: u64,
    /// Crossings the gate below refused to re-fire. A large number here is
    /// normal and healthy — it is the same standing crossing seen again.
    n_tt_gated: u64,
    n_tt_fired: u64,
    /// Crossings refused because their whole net edge fitted inside what the
    /// HEDGE is licensed to give away. Counted per book event, like
    /// `take_take_found` and for the same reason: a crossing stands until
    /// someone takes it, so this is "how often we saw one", not "how many".
    ///
    /// This is the only evidence that would ever calibrate `--hedge-max-slip`,
    /// and without it the shadow run — whose whole job is to measure before
    /// money is risked — cannot tell "no crossing existed" from "a crossing
    /// was refused". `Skip::NetUnderSlip` is a PRICE fact that moves tick to
    /// tick, unlike an infeasible pairing, which is a config fact and is
    /// reported once at startup instead.
    n_tt_under_slip: u64,
    /// Per quoter index: may take-take pair this relationship's two legs at
    /// all? `crate::taketake::pairing_pays`, asked once here because it is a
    /// registry fact and answering it per book event allocates.
    tt_feasible: Vec<bool>,
    tt_gate: crate::taketake::Gate,
    /// The bar is re-derived from marks on the stats tick: it moves as the book
    /// turns over, and a stale bar is a wrong bar in both directions.
    ///
    /// `None` is a REFUSAL to run take-take, not a missing number (see
    /// `taketake::Bar`). It used to be `unwrap_or(DEFAULT_BAR_APR)` over a plain
    /// read, so when marks.json froze at 12:46:12 on 2026-07-28 the armed
    /// session spent four hours firing against that frozen 10.0088%/yr bar and
    /// said nothing about it.
    tt_bar: Option<f64>,
    next_oid: u64,
    next_hedge_oid: u64,
    next_tt_oid: u64,
    killed: bool,
    /// Feed-health pull (card 0a7e5478). Holds the REASON, not just a flag, so
    /// a pulled engine can always say why it is silent. Starts pulled when the
    /// check is on: we have not yet proven the feeds are healthy, and the first
    /// tick either clears it or names the problem.
    feed_reason: Option<String>,
    /// The engine's own subscription to the recorder, tracked separately from
    /// what the recorder says about the venues: a bench tape cannot disconnect,
    /// a socket can and did (ten times on 2026-07-28).
    link: Link,
    /// Last `stale_seconds_total` read per required feed. Differencing that
    /// monotone counter is what makes the health check a DETECTOR rather than a
    /// five-second sampler over a one-second flag — see `feed_stale_reason`.
    stale_seen: HashMap<String, f64>,
    last_now: f64,
    chan_hw: usize,
    fills: FillLedger,
    /// order id -> (relationship id, class). A fill arrives with our order id
    /// only, but exposure is booked per relationship, so the mapping is captured
    /// at place time when the rel is in hand.
    order_rel: HashMap<String, MakerOrder>,
    /// venue's order id -> ours, learned from order_ack. Read by the FILL path:
    /// a venue reports a fill under its own id.
    venue_oid: HashMap<String, String>,
    /// ...and ours -> the venue's, learned from the same ack. Read by the CANCEL
    /// path: both venues' cancel endpoints accept only their own id, and until
    /// 2026-07-28 this map did not exist, so every per-order cancel the engine
    /// ever sent was addressed to an id the venue had never issued.
    oid_venue: HashMap<String, String>,
    /// Cancels waiting on the ack that will make them addressable, by OUR id.
    parked_cancels: HashMap<String, ParkedCancel>,
    n_cancel_escalated: u64,
    /// Every hedge ATTEMPT we placed, by OUR id — superseded ones included, so a
    /// late frame on one is still recognisable. Deliberately separate from the
    /// FillLedger: a hedge in the ledger would hedge its own fill.
    hedge_orders: HashMap<String, HedgeOrder>,
    /// Outstanding hedge OBLIGATIONS, keyed by the id of the first attempt made
    /// for each (see `PendingHedge` for the invariant it maintains). Populated
    /// for every obligation, retry policy or not: it is the accounting unit, and
    /// `hedged_by_maker` — one credit shared by every obligation of a maker
    /// order — is what it replaces.
    pending_hedges: HashMap<String, PendingHedge>,
    /// Fills we cannot attribute yet, by the id the venue reported. See
    /// `UnclaimedFill`: held for their ack, alarmed if it never comes.
    unclaimed_fills: HashMap<String, UnclaimedFill>,
    n_retry: u64,
    n_naked: u64,
    /// Fills that expired unclaimed (money we cannot explain) and hedge fills
    /// beyond what an obligation owed. Both must stay 0.
    n_unattributed: u64,
    n_overhedge: u64,
    n_ack: u64,
    n_fill: u64,
    n_hedge: u64,
}

impl Engine {
    fn new(
        cfg: RunCfg,
        exec_txs: HashMap<Venue, mpsc::Sender<ExecCmd>>,
        exec_stats: Arc<ExecStats>,
        by_market: &ByMarket,
        quoters: &[Quoter],
    ) -> Engine {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        // Which relationships take-take may pair at all. A registry fact, so it
        // is settled here rather than re-derived per book event — and it is
        // LOUD when it refuses one, because a rel that is quoted, vetted and
        // permanently untradable by this path is something an operator has to
        // know about. Nothing in today's registry trips it.
        let tt_feasible: Vec<bool> =
            quoters.iter().map(|q| crate::taketake::pairing_pays(&q.rel)).collect();
        for (q, ok) in quoters.iter().zip(&tt_feasible) {
            if !ok {
                eprintln!(
                    "[take-take] {} is `{}` and pays nothing in some feasible state with the \
                     Kalshi leg long — never tradable by take-take, skipped for the life of \
                     this process",
                    q.rel.id,
                    q.rel.rtype.as_str()
                );
            }
        }
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
        let feed_reason: Option<String> =
            cfg.health_file.is_some().then(|| "startup — feeds not yet proven healthy".to_string());
        let link = if cfg.bench { Link::Fresh } else { Link::Down };
        let required = required_feeds(by_market);
        let t_start = std::time::Instant::now();
        let wal = cfg.wal_path.as_deref().map(Wal::spawn);
        let out = cfg.out_path.as_ref().map(|p| {
            if let Some(dir) = std::path::Path::new(p).parent() {
                std::fs::create_dir_all(dir).expect("out dir");
            }
            std::io::BufWriter::new(
                std::fs::OpenOptions::new().create(true).append(true).open(p).expect("out"),
            )
        });
        Engine {
            cfg,
            exec_txs,
            exec_stats,
            cx,
            fees,
            books: BookBuilder::new(),
            digest: Sha256::new(),
            decision: Hist::new(),
            intents: Vec::new(),
            out,
            wal,
            t_start,
            required,
            n_ev: 0,
            n_book: 0,
            n_int: 0,
            n_tt: 0,
            n_tt_gated: 0,
            n_tt_fired: 0,
            n_tt_under_slip: 0,
            tt_feasible,
            tt_gate: crate::taketake::Gate::default(),
            tt_bar,
            next_oid: id_base,
            next_hedge_oid: id_base,
            next_tt_oid: id_base,
            killed: false,
            feed_reason,
            link,
            stale_seen: HashMap::new(),
            last_now: 0.0,
            chan_hw: 0,
            fills: FillLedger::new(),
            order_rel: HashMap::new(),
            venue_oid: HashMap::new(),
            oid_venue: HashMap::new(),
            parked_cancels: HashMap::new(),
            n_cancel_escalated: 0,
            hedge_orders: HashMap::new(),
            pending_hedges: HashMap::new(),
            unclaimed_fills: HashMap::new(),
            n_retry: 0,
            n_naked: 0,
            n_unattributed: 0,
            n_overhedge: 0,
            n_ack: 0,
            n_fill: 0,
            n_hedge: 0,
        }
    }

    /// Queue one effect command on its venue's executor. Returns false when the
    /// channel is full — the command is LOST and only the counter moves, which
    /// is why callers with an ordered sequence must stop on a false.
    fn dispatch(&self, venue: Venue, action: Action) -> bool {
        match self.exec_txs.get(&venue) {
            Some(tx) => {
                let queued =
                    tx.try_send(ExecCmd { t_read: std::time::Instant::now(), action }).is_ok();
                if !queued {
                    self.exec_stats.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                queued
            }
            None => false,
        }
    }

    /// Emit every pending intent and route its effect to a venue executor.
    ///
    /// `rel`: the relationship whose quoter emitted these intents (for the
    /// hedge-anchor lookup at place time), or None for intents that rest
    /// nothing (hedge obligations).
    ///
    /// `to_line` is the ONE serialisation of an intent in the process. This
    /// used to serialise here and then immediately `from_str` the result back
    /// into a `Value` to decide what to do with it, so the routing below read
    /// every field through `.and_then(|x| x.as_str()).unwrap_or("")` — a
    /// missing price became `"0"` and a missing side became a BID.
    fn drain_intents(&mut self, rel: Option<&Rel>) {
        // Swapped out of `self` rather than drained in place: the body below
        // needs `&mut self`, and swapped BACK rather than reallocated because
        // this runs on every book event. Nothing in the body pushes an intent.
        let mut pending = std::mem::take(&mut self.intents);
        for it in pending.drain(..) {
            let l = it.to_line();
            self.digest.update(l.as_bytes());
            self.digest.update(b"\n");
            self.n_int += 1;
            if let Some(o) = self.out.as_mut() {
                writeln!(o, "{l}").expect("write out");
                if !self.cfg.bench {
                    o.flush().expect("flush out"); // tail -f visibility; ~80/day live
                }
            }
            // route the effect to its venue executor (dry-run gateway seam)
            match &it {
                // fill-ledger bookkeeping: orders enter the ledger at place
                // time carrying their quote-time hedge anchor, so a later
                // fill knows where to hedge without re-reading the book.
                Intent::Place(p) => {
                    // A hedge is never registered: it has no hedge of its
                    // own, and registering it would make its fill mint one.
                    if p.tag != Some(Tag::Hedge) {
                        let anchor =
                            rel.and_then(|r| hedge_anchor(r, &p.place, p.side, &self.books));
                        self.fills.register_order(&p.order_id, &p.place, p.count, anchor);
                    }
                    if let Some(r) = rel {
                        self.order_rel.insert(
                            p.order_id.clone(),
                            MakerOrder {
                                rel_id: r.id.clone(),
                                class: r.rtype.as_str(),
                                venue: p.venue.as_str().to_string(),
                                market_id: p.place.clone(),
                                side: p.side,
                                price: p.price.clone(),
                                // The intent already carries who emitted
                                // it; the ledger just has to stop
                                // discarding that.
                                strategy: match p.tag {
                                    Some(Tag::TakeTake) => "take-take",
                                    _ => "maker-hedge",
                                },
                            },
                        );
                    }
                    // an amend retires the old id, but a fill can still
                    // race it — observe_cancel KEEPS the record.
                    if let Some(roid) = &p.replaces {
                        self.fills.observe_cancel(roid);
                    }
                }
                Intent::Cancel(c) => self.fills.observe_cancel(&c.order_id),
                // Records, not orders: nothing rests and nothing is pulled.
                Intent::HedgeNeeded(_) | Intent::Skip(_) => {}
            }
            // Build the REAL requests from the intent (see `intent_actions`:
            // an amend is a cancel AND a place, in that order). The venue is
            // read off the intent, so the executor a command is SENT to and
            // the intent it was BUILT from cannot name different venues.
            if let Some(venue) = it.venue() {
                // Only the park reads this clock, and only an armed engine
                // parks, so a replay's determinism cannot depend on it.
                let now = std::time::Instant::now();
                // The order this intent's cancel is owed FOR, so the dispatch
                // below can record the attempt against its park entry. The
                // cancel is parked when it is decided and discharged only when
                // the venue answers — see `cancel::resolve_cancel`.
                let owed = cancel::cancel_target(&it).map(str::to_string);
                for action in intent_actions(
                    &it,
                    self.cfg.armed,
                    &self.oid_venue,
                    &mut self.parked_cancels,
                    now,
                ) {
                    let is_cancel = matches!(action, Action::Cancel { .. });
                    let queued = self.dispatch(venue, action);
                    if is_cancel {
                        if let Some(oid) = owed.as_deref() {
                            cancel::mark_sent(&mut self.parked_cancels, oid, queued);
                        }
                    }
                    if !queued {
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
        self.intents = pending;
    }

    /// Stop quoting AND leave nothing resting — the same standard the KILL path
    /// was held to on 2026-07-28, for the same reason.
    ///
    /// `cancel_all` alone is not enough: it reaches only orders the engine still
    /// holds ids for, and NONE of its cancels is verified. The feed-stale pull
    /// did exactly that and no more, so `feed_pulled: true` could sit over real
    /// quotes resting on a book the engine could no longer see — the one state
    /// this pull exists to prevent. The venue sweep is the part that cannot be
    /// fooled: it reaches orders we hold no id for at all, and it PROVES the
    /// outcome.
    fn pull_quotes(&mut self, quoters: &mut [Quoter], why: &str) {
        for q in quoters.iter_mut() {
            q.cancel_all(&mut self.cx, self.last_now, &mut self.intents);
            self.drain_intents(Some(&q.rel));
        }
        // Empty unarmed — an unarmed engine has no executor and nothing at
        // a venue to sweep.
        for (venue, tx) in self.exec_txs.iter() {
            if tx
                .try_send(ExecCmd {
                    t_read: std::time::Instant::now(),
                    action: Action::SweepAndVerify,
                })
                .is_err()
            {
                eprintln!(
                    "[engine] {why}: could not queue sweep for {venue:?} — executor \
                     backlogged; book NOT proven clean"
                );
            }
        }
    }

    fn summary(&self) -> serde_json::Value {
        let elapsed = self.t_start.elapsed().as_secs_f64();
        serde_json::json!({
            "mode": if self.cfg.bench { "bench" } else if self.cfg.armed { "live" } else { "shadow" },
            "events": self.n_ev, "book_events": self.n_book, "intents": self.n_int,
            "take_take_found": self.n_tt, "take_take_bar_apr": self.tt_bar,
            "take_take_gated": self.n_tt_gated, "take_take_fired": self.n_tt_fired,
            "take_take_net_under_slip": self.n_tt_under_slip,
            "killed": self.killed,
            "feed_pulled": self.feed_reason.is_some(),
            "risk_allowed": self.cfg.risk.as_ref().map(|r| r.stats().0).unwrap_or(0),
            "risk_rejected": self.cfg.risk.as_ref().map(|r| r.stats().1).unwrap_or(0),
            // Cancels the engine has DECIDED and the venue has not yet
            // confirmed: waiting on an ack to become addressable, on the wire,
            // or refused and due for a retry. Healthy is 0, or a transient
            // handful; a number that does not come back down is orders resting
            // that this engine has already decided against.
            "cancels_unresolved": self.parked_cancels.len(),
            // ...of which these have already had their one client-id
            // escalation and are still unaddressable. This is the subset a
            // human has to reason about, and it is separated out precisely so
            // the gauge above stays a real signal: the commonest member is a
            // place the venue REJECTED, where nothing rests and nothing is
            // wrong (see the escalation log line).
            "cancels_unaddressable":
                self.parked_cancels.values().filter(|p| p.escalated).count(),
            "cancels_escalated": self.n_cancel_escalated,
            "hedges_pending": self.pending_hedges.len(),
            // ...of which THIS process knows nothing: contracts a previous run
            // owed a hedge for and never booked. `hedges_pending` counts only
            // what is live in memory, so it read 0 after the 01:34 restart on
            // 2026-07-29 while a PM-US short was still real at the venue. This
            // is the standing signal that it is not the whole story;
            // arbbot-hedge.timer is what completes them (see `orphan`).
            "hedges_undischarged": self.cfg.hedges_undischarged,
            "hedges_retried": self.n_retry,
            "hedges_naked": self.n_naked,
            // Hedge contracts filled beyond what an obligation owed — a
            // position with no maker leg to pair it with. Must stay 0.
            "hedges_overfilled": self.n_overhedge,
            "order_acks": self.n_ack, "fills": self.n_fill, "hedge_obligations": self.n_hedge,
            // Fills held for the `order_ack` that would name them. A
            // transient 1 is the ack race; a persistent count is a broken
            // ack path.
            "fills_unclaimed": self.unclaimed_fills.len(),
            // ...and fills that gave up waiting: money that moved in our
            // account that we cannot explain. Must stay 0.
            "fills_unattributed": self.n_unattributed,
            // programming-bug alarm: an obligation that was minted and
            // never hedged (arb_core::fill) — must stay 0.
            "dropped_unconsumed": dropped_unconsumed(),
            // Windows in which a Kalshi fill may have gone unseen: the boot
            // window plus every reconnect, so 1 is the FLOOR and 0 means the
            // feed never started. That feed sums per-fill deltas locally and
            // its state does not survive a restart, so a gap is only harmless
            // if the venue replays on resubscribe — unestablished (see
            // `fills`). Every other gauge here is blind to it: no frame
            // arrived, so nothing was unattributed and no obligation was
            // minted. This one is per-process and NOT seeded from persisted
            // state, unlike `hedges_undischarged` above, so it says "there was
            // a window", never "nothing was lost". `fills::kalshi_fill_gaps`
            // carries the runbook for checking a window against venue truth.
            "kalshi_fill_gaps": crate::fills::kalshi_fill_gaps(),
            // Kalshi fill frames whose count could not be read — a payload
            // shape change, which this field family has had before. Must stay
            // 0: while it is rising, `fills` reads 0 and every other gauge
            // here looks healthy.
            "kalshi_fills_unreadable": crate::fills::kalshi_fills_unreadable(),
            "would_place": self.exec_stats.placed.load(std::sync::atomic::Ordering::Relaxed),
            "would_cancel": self.exec_stats.cancelled.load(std::sync::atomic::Ordering::Relaxed),
            "exec_dropped": self.exec_stats.dropped.load(std::sync::atomic::Ordering::Relaxed),
            "exec_sent": self.exec_stats.sent.load(std::sync::atomic::Ordering::Relaxed),
            "exec_failed": self.exec_stats.failed.load(std::sync::atomic::Ordering::Relaxed),
            // Orders the venue took while we could not read the answer, found
            // resting and adopted. Must stay 0; each one was live under an id
            // this process had not learned.
            "exec_recovered": self.exec_stats.recovered.load(std::sync::atomic::Ordering::Relaxed),
            "chan_high_water": self.chan_hw,
            "decision_latency": self.decision.summary(),
            "exec_hop_latency": self.exec_stats.hop.summary(),
            "elapsed_s": (elapsed * 10.0).round() / 10.0,
            "eps": if elapsed > 0.0 { (self.n_ev as f64 / elapsed) as u64 } else { 0 },
        })
    }

    /// One line off the feed channel. `queued` is the channel depth behind it.
    fn on_feed(
        &mut self,
        m: FeedMsg,
        queued: usize,
        quoters: &mut [Quoter],
        by_market: &ByMarket,
    ) {
        self.n_ev += 1;
        self.chan_hw = self.chan_hw.max(queued);
        // THE merge point: everything that reaches the engine passes
        // here exactly once, so this is where the WAL sequence is
        // assigned — before any parsing, so lines the engine skips are
        // still in the incident record verbatim.
        if let Some(w) = self.wal.as_mut() {
            w.append(&m.line);
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&m.line) else { return };
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
        if !self.cfg.bench && (kind == crate::feed::FEED_UP || kind == crate::feed::FEED_DOWN) {
            self.on_link_line(&v, kind, quoters);
            return;
        }
        let Some(venue) = v.get("venue").and_then(|x| x.as_str()).and_then(Venue::parse) else {
            return;
        };
        let Some(market_id) = v.get("market_id").and_then(|x| x.as_str()).map(str::to_owned) else {
            return;
        };
        let seq = v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0);
        let ts_local_ns = v.get("ts_local_ns").and_then(|x| x.as_i64()).unwrap_or(0);
        let ts_venue = v.get("ts_venue").and_then(|x| x.as_str()).map(str::to_owned);
        match kind {
            "snapshot" => {
                let (Some(bids), Some(asks)) = (levels_of(v.get("bids")), levels_of(v.get("asks")))
                else {
                    return;
                };
                self.books.apply_snapshot(
                    venue,
                    &market_id,
                    bids,
                    asks,
                    seq,
                    ts_local_ns,
                    ts_venue,
                );
                // Evidence that a reconnect's welcome burst is really
                // arriving — the only thing that clears the pull a
                // disconnect set (see `Link`).
                if let Link::Resyncing { snapshots, .. } = &mut self.link {
                    *snapshots += 1;
                }
            }
            "delta" => {
                // THE inbound boundary for a side: an untrusted venue tape
                // line. Parsed once, here, and a line whose side is neither
                // spelling is dropped rather than guessed at.
                let Some(side) = v.get("side").and_then(|x| x.as_str()).and_then(BookSide::parse)
                else {
                    return;
                };
                let (Some(price), Some(size)) = (
                    v.get("price").and_then(|x| x.as_str()),
                    v.get("size").and_then(|x| x.as_str()),
                ) else {
                    return;
                };
                match self.books.apply_delta(
                    venue,
                    &market_id,
                    side,
                    price,
                    size,
                    seq,
                    ts_local_ns,
                    ts_venue,
                ) {
                    Ok(_) => {}
                    Err(ApplyError::GapDetected { .. }) | Err(ApplyError::NotSynced) => return,
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
                self.on_order_ack(&v, ts_local_ns, m.t_read);
                return;
            }
            // ...and the venue's answer to a CANCEL:
            //   {"kind":"cancel_result","venue":...,"market_id":str,
            //    "venue_order_id"|"order_id":str,"ok":bool,"error":str|null,
            //    "ts_local_ns":int}
            // The id is reported in the space the cancel was addressed in. It
            // is the only way the engine can learn that a cancel it owes has
            // actually been carried out — or refused.
            "cancel_result" => {
                self.on_cancel_result(&v, m.t_read);
                return;
            }
            "fill" => {
                self.on_fill(&v, venue, &market_id, ts_local_ns, m.t_read);
                return;
            }
            _ => return,
        }
        self.n_book += 1;
        let now = ts_local_ns as f64 / 1e9;
        self.last_now = now;
        if !self.killed && self.feed_reason.is_none() {
            if let Some(idxs) = by_market.get(&(venue, market_id)) {
                self.quote(quoters, idxs, now);
                // Take-take on the SAME event that moved the book: the
                // crossing exists for as long as the slower side takes
                // to react, which is not minutes.
                self.take_take_scan(quoters, idxs, now);
            }
        }
        self.decision.record(m.t_read.elapsed().as_nanos() as u64);
    }

    /// A feed-connection control line: our own subscription came up or went
    /// away.
    fn on_link_line(&mut self, v: &serde_json::Value, kind: &str, quoters: &mut [Quoter]) {
        if kind == crate::feed::FEED_UP {
            // NOT a clear. `resync_reason` decides when the welcome
            // burst has actually made the books current again; this
            // only starts the clock for it.
            self.link = Link::Resyncing { since: std::time::Instant::now(), snapshots: 0 };
            eprintln!(
                "[engine] feed reconnected — quotes stay pulled until the welcome \
                 snapshot burst has landed"
            );
            return;
        }
        // Act on the DOWN EDGE only: `socket_feed` re-emits
        // FEED_DOWN every 2s for as long as the recorder is
        // unreachable.
        let edge = !matches!(self.link, Link::Down);
        self.link = Link::Down;
        if !edge {
            return;
        }
        // ...and sweep only on the way IN, the same rule the
        // health tick follows. Nothing can have started
        // resting while the engine was already pulled, and a
        // flapping subscriber would otherwise sweep every
        // reconnect against the rate budget the order path
        // needs.
        let entering = self.feed_reason.is_none();
        self.feed_reason = resync_reason(&self.link, std::time::Instant::now());
        eprintln!(
            "[engine] FEED DOWN ({}) — quotes pulled",
            v.get("note").and_then(|x| x.as_str()).unwrap_or("no reason given")
        );
        if entering {
            self.pull_quotes(quoters, "FEED DOWN");
        }
    }

    /// Re-quote every relationship this book event touches.
    fn quote(&mut self, quoters: &mut [Quoter], idxs: &[usize], now: f64) {
        for &qi in idxs {
            quoters[qi].on_book(
                &mut self.cx,
                &self.fees,
                &self.books,
                now,
                &mut self.next_oid,
                &mut self.intents,
            );
            self.drain_intents(Some(&quoters[qi].rel));
        }
    }

    /// Immediately-executable crossings on this book event.
    ///
    /// No trustworthy bar, no take-take. `tt_bar` is `None` exactly when
    /// `marks.json` is present but stale or corrupt, and the bar IS the
    /// profitability test — a frozen one is wrong in both directions, so there
    /// is no substitute to fall back to.
    ///
    /// No HEDGE POLICY, no take-take either. Only leg 1 is placed here; leg 2
    /// is a hedge, and `detect` now requires the net edge to outlast what that
    /// hedge is licensed to give away. Without a policy there is no budget to
    /// price against — and `first_attempt_acceptable` is UNGATED in that case,
    /// so leg 2 could fill anywhere. `run_cfg` gates both on `!bench`, so this
    /// refusal is unreachable in production and is here to stay that way.
    fn take_take_scan(&mut self, quoters: &mut [Quoter], idxs: &[usize], now: f64) {
        let (Some(tt), Some(tt_bar), Some(pol)) =
            (self.cfg.take_take.as_ref(), self.tt_bar, self.cfg.hedge_retry.as_ref())
        else {
            return;
        };
        // Copied out of the policy rather than held as a borrow of `self.cfg`:
        // the loop below drains intents, which needs `&mut self`. `marks_path`
        // is not read here — only the stats tick re-derives the bar.
        let (max_ct_per_rel, max_clip, detect_only, cooldown_s) =
            (tt.max_ct_per_rel, tt.max_clip, tt.detect_only, tt.cooldown_s);
        let max_slip = pol.max_slip.clone();
        let today = crate::taketake::today_iso(now);
        for &qi in idxs {
            // `detect` reads this relationship's two legs as two spellings of
            // one claim, which is only true of some types. Settled at startup
            // (and logged there) because it is a registry fact.
            if !self.tt_feasible[qi] {
                continue;
            }
            let open =
                self.cfg.risk.as_ref().map(|r| r.open_ct(&quoters[qi].rel.id)).unwrap_or(0.0) as i64;
            let found = crate::taketake::detect(
                &mut self.cx,
                &quoters[qi].rel,
                &self.books,
                &today,
                tt_bar,
                max_ct_per_rel,
                open,
                max_clip,
                &max_slip,
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
                    self.intents.push(Intent::Skip(intent::Skip {
                        skip: vec![format!(
                            "crossed book {venue} take-take {}",
                            quoters[qi].rel.id
                        )],
                        ts: now,
                    }));
                    self.drain_intents(Some(&quoters[qi].rel));
                    continue;
                }
                // A crossing that was real on screen and that the hedge could
                // legally erase. The ONLY evidence that would ever calibrate
                // `--hedge-max-slip`, and the detect-only shadow is blind
                // without it, so it is counted where the rest are dropped.
                Err(crate::taketake::Skip::NetUnderSlip { .. }) => {
                    self.n_tt_under_slip += 1;
                    continue;
                }
                Err(_) => continue,
                Ok(c) => c,
            };
            self.n_tt += 1;
            // The SAME crossing is present on every event
            // until someone takes it, and exposure does not
            // move until a fill books. Without this the
            // armed path would re-place it every tick.
            if !self.tt_gate.take(&c.rel_id, now, cooldown_s) {
                self.n_tt_gated += 1;
                continue;
            }
            if detect_only {
                eprintln!(
                    "[take-take] FOUND {} x{} edge={} net={} apr={:.0}%/yr \
                     (bar {:.0}%) — buy {} @{} / sell {} @{} \
                     [DETECT ONLY — nothing sent]",
                    c.rel_id,
                    c.size,
                    c.edge,
                    c.net,
                    c.apr,
                    tt_bar,
                    c.kalshi_market,
                    c.kalshi_ask,
                    c.pmus_market,
                    c.pmus_bid,
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
            if let Some(rv) = self.cfg.risk.as_ref() {
                // The venue named here is the one being QUOTED,
                // which is now leg 1's rather than always PM-US.
                // Behaviour is unchanged either way — `venue_costs`
                // charges every venue the basket spends on, so a
                // two-venue relationship reserves both whichever leg
                // leads (risk.rs) — but the refusal reason should
                // name the leg we were about to send first.
                let v = rv.check(&quoters[qi].rel, c.lead, c.size);
                if !v.allowed {
                    eprintln!(
                        "[take-take] REFUSED {} x{} apr={:.0}%/yr — {}",
                        c.rel_id,
                        c.size,
                        c.apr,
                        v.reasons.join("; ")
                    );
                    continue;
                }
            }
            // Leg 1 ONLY, and it is `Candidate::lead` — the
            // leg whose touch is likeliest to vanish before we
            // reach it — rather than always PM-US. Its fill
            // mints the OTHER leg through the same anchor path
            // a maker fill uses, so leg 2 inherits retry,
            // escalation, the naked alarm and ledger booking
            // rather than duplicating them; `hedge_anchor`
            // derives that leg from `rel.legs` and is symmetric
            // in the direction, so both leads reach it.
            // `taker` makes it a marketable IOC.
            let (leg1_venue, leg1_market, leg1_price, leg1_side) = c.leg1();
            let (leg1_market, leg1_price) = (leg1_market.to_string(), leg1_price.to_string());
            self.next_tt_oid += 1;
            self.n_tt_fired += 1;
            let (first, second) = if c.lead == Venue::Kalshi {
                (
                    format!("buy {} @{}", c.kalshi_market, c.kalshi_ask),
                    format!("sell {} @{}", c.pmus_market, c.pmus_bid),
                )
            } else {
                (
                    format!("sell {} @{}", c.pmus_market, c.pmus_bid),
                    format!("buy {} @{}", c.kalshi_market, c.kalshi_ask),
                )
            };
            eprintln!(
                "[take-take] FIRE {} x{} edge={} net={} apr={:.0}%/yr \
                 (bar {:.0}%) — leg 1 {first} then leg 2 {second}",
                c.rel_id, c.size, c.edge, c.net, c.apr, tt_bar,
            );
            self.intents.push(Intent::Place(intent::Place {
                count: c.size,
                old_price: None,
                order_id: format!("t{}", self.next_tt_oid),
                place: leg1_market,
                price: leg1_price,
                replaces: None,
                retry: None,
                side: leg1_side,
                tag: Some(Tag::TakeTake),
                taker: true,
                ts: now,
                venue: leg1_venue,
            }));
            self.drain_intents(Some(&quoters[qi].rel));
        }
    }

    /// The kill-switch watch.
    fn kill_tick(&mut self, quoters: &mut [Quoter]) {
        let kill_now = std::path::Path::new(&self.cfg.kill_file).exists();
        if kill_now && !self.killed {
            self.killed = true;
            eprintln!(
                "[engine] KILL switch on ({}) — cancelling all resting quotes",
                self.cfg.kill_file
            );
            for q in quoters.iter_mut() {
                q.cancel_all(&mut self.cx, self.last_now, &mut self.intents);
                self.drain_intents(Some(&q.rel));
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
            for (venue, tx) in self.exec_txs.iter() {
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
        } else if !kill_now && self.killed {
            self.killed = false;
            eprintln!("[engine] KILL switch cleared — quoting resumes");
        }
    }

    /// The stats line, and the take-take bar it re-derives.
    fn stats_tick(&mut self) {
        println!("{}", self.summary());
        if let Some(o) = self.out.as_mut() {
            o.flush().expect("flush");
        }
        // Re-derive the bar: marks are rewritten by arbbot-marks.timer
        // as the book turns over, and holding the startup value would
        // let the engine trade against a stale definition of "good".
        if let Some(tt) = self.cfg.take_take.as_ref() {
            let bar = read_bar(&tt.marks_path);
            let now_bar = bar.tradable();
            // Edge-triggered, like `feed_reason`: the four hours the
            // armed session spent firing against a frozen bar produced
            // not one line saying so, and a line every stats tick is a
            // line nobody reads. `take_take_bar_apr` in the summary
            // above renders `None` as null, which is the standing
            // signal.
            if now_bar.is_some() != self.tt_bar.is_some() {
                eprintln!("[take-take] {}", bar.describe());
            }
            self.tt_bar = now_bar;
        }
    }

    /// The feed has closed: flush, alarm on anything still held, and report.
    fn finish(&mut self) -> serde_json::Value {
        if let Some(o) = self.out.as_mut() {
            o.flush().expect("final flush");
        }
        // Anything still HELD has run out of chances to be explained: the loop is
        // over, so no `order_ack` is coming for it. Say so once per fill rather than
        // letting the process exit with the count buried in a gauge — and count it,
        // so a bench/replay (which never runs the deadline above) still reports it.
        for (id, u) in std::mem::take(&mut self.unclaimed_fills) {
            self.n_unattributed += 1;
            eprintln!(
                "[fill] UNEXPLAINED at exit: {}x on {} {} reported as order {id}, never claimed by \
                 an order_ack. Money moved in this account that the engine cannot attribute — \
                 RECONCILE BY HAND.",
                u.cum,
                u.venue.as_str(),
                u.market_id
            );
        }
        let mut s = self.summary();
        if self.cfg.bench {
            let digest = std::mem::take(&mut self.digest);
            s["sha256"] = serde_json::json!(format!("{:x}", digest.finalize()));
        }
        s
    }
}

/// How many feed events `run` may process before the deadline arms below are
/// owed a turn.
///
/// `biased` polls the select's arms in declaration order and takes the first
/// READY one, and `rx.recv()` on a non-empty channel is always ready — so with
/// no cap the kill switch, the hedge retry and the naked alarm are not
/// deadlines at all, they are when-idle callbacks. Backlogs are structural, not
/// hypothetical: the recorder rebroadcasts a snapshot for every book it holds
/// every 30s and sends a ~1.4MB burst on connect, and `socket_feed` pushes all
/// of it into the 65536-deep channel unpaced. The armed engine reported
/// `chan_high_water: 1036` and a `decision_latency` max of 6_496_952_349 ns on
/// 2026-07-29 — 6.5 seconds in which `data/KILL` was never stat'ed,
/// `health_tick` did not run, and no hedge retry or naked alarm could fire. The
/// thing that stopped the naked alarm was the market feed misbehaving, which is
/// the one condition it exists to survive (see `engine::hedge`).
///
/// 64 is small enough to bound the halt at well under its 1s interval even at
/// the drain rate that 6.5s window implies, and costs one extra loop iteration
/// and six timer polls per 64 events — noise against a JSON parse per event.
const FEED_BUDGET: usize = 64;

pub async fn run(
    mut quoters: Vec<Quoter>,
    by_market: HashMap<(Venue, String), Vec<usize>>,
    mut rx: mpsc::Receiver<FeedMsg>,
    exec_txs: HashMap<Venue, mpsc::Sender<ExecCmd>>,
    exec_stats: Arc<ExecStats>,
    cfg: RunCfg,
) -> serde_json::Value {
    let bench = cfg.bench;
    let armed = cfg.armed;
    let hedge_retry = cfg.hedge_retry.is_some();
    let stats_every_s = cfg.stats_every_s;
    let mut eng = Engine::new(cfg, exec_txs, exec_stats, &by_market, &quoters);

    let mut kill_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    kill_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stats_iv = tokio::time::interval(std::time::Duration::from_secs(stats_every_s.max(1)));
    stats_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut feed_iv = tokio::time::interval(std::time::Duration::from_secs(5));
    feed_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut hedge_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    hedge_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cancel_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    cancel_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut fill_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    fill_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Unbounded in bench/replay: the budget never reaches zero, so both guards
    // below are constant, the select polls exactly the arms it polled before in
    // exactly the same order, and the digest is unchanged by construction. A
    // cap that reordered replay would be a worse defect than the one it fixes.
    let mut budget = if bench { usize::MAX } else { FEED_BUDGET };

    loop {
        tokio::select! {
            biased;
            msg = rx.recv(), if budget > 0 => {
                let Some(m) = msg else { break }; // feed closed (bench EOF)
                budget -= 1;
                let queued = rx.len();
                eng.on_feed(m, queued, &mut quoters, &by_market);
            }
            _ = kill_iv.tick() => eng.kill_tick(&mut quoters),
            _ = hedge_iv.tick(), if hedge_retry && !bench => eng.hedge_tick(),
            // Fills held for an `order_ack` that has not come. Bench has no live
            // ack path at all and must stay byte-deterministic, so it relies on
            // the flush after the loop instead of this deadline.
            _ = fill_iv.tick(), if !bench => eng.unclaimed_tick(),
            // Cancels the engine owes but could not address when it decided on
            // them. Only an armed engine can ever learn a venue id, so only an
            // armed engine parks (see `resolve_cancel`) and only it has anything
            // to do here. `cancel_work` owns the policy — including the one
            // escalation per tick and none at all while killed.
            _ = cancel_iv.tick(), if armed => eng.cancel_tick(),
            // Two independent facts, in order of locality: whether the engine's
            // own subscription can be trusted, then whether the recorder says
            // the venue sockets can be. Ungated by `--health` (only by bench)
            // because the FIRST of those is the engine's own business — a run
            // without a health file must still be able to notice, and clear, a
            // disconnect of its own feed.
            _ = feed_iv.tick(), if !bench => eng.health_tick(&mut quoters),
            _ = stats_iv.tick(), if !bench => eng.stats_tick(),
            // The budget is spent and every deadline that was DUE has now had
            // its turn: the arms above are polled first and this one is always
            // ready, so it is reached only once none of them will fire. Refill
            // and go back to the feed. It is `ready` rather than another timer
            // because a deadline that is not due must not stall the feed —
            // trading a 6.5s halt latency for a 1s decision latency is not a
            // fix, it is the same defect pointing the other way.
            _ = std::future::ready(()), if budget == 0 => budget = FEED_BUDGET,
        }
    }

    eng.finish()
}

/// A `RunCfg` for a unit test: nothing that could reach a venue, write the
/// accounting ledger, read a health file, or consult a risk view.
///
/// `bench: true` is here for the id seed and nothing else. `id_base` is 0 in
/// bench, so the order ids a test asserts on are `h1`, `h2`, ... rather than a
/// wall-clock offset no test can predict. Every deadline a test drives is
/// called directly, so the `if !cfg.bench` guards in `run()`'s select do not
/// apply to it.
#[cfg(test)]
fn test_cfg() -> RunCfg {
    RunCfg {
        out_path: None,
        kill_file: "/nonexistent/KILL".into(),
        stats_every_s: 86_400,
        bench: true,
        wal_path: None,
        health_file: None,
        risk: None,
        ledger_path: None,
        hedge_retry: None,
        take_take: None,
        armed: false,
        hedges_undischarged: 0,
    }
}

/// An `Engine` with no executors at all, so nothing a test does can reach a
/// venue by construction.
///
/// This exists because `attribute_fill` and the hedge tick are ordinary
/// methods now. While they were macro bodies the only way to reach them was to
/// spawn `run()` over a real channel and infer what had happened from the
/// summary — which is why the two of them between them carried seven defects
/// into production with a green test suite.
#[cfg(test)]
fn test_engine(cfg: RunCfg) -> Engine {
    use std::sync::atomic::AtomicU64;
    Engine::new(
        cfg,
        HashMap::new(),
        Arc::new(ExecStats {
            hop: Hist::new(),
            placed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            recovered: AtomicU64::new(0),
        }),
        &HashMap::new(),
        // These tests drive the hedge and fill paths directly, never
        // `take_take_scan`, which is the only reader of the feasibility table.
        &[],
    )
}

#[cfg(test)]
mod take_take_wiring_tests {
    use super::*;
    use arb_core::scan::{RelLeg, RelType};

    /// One vetted cross-venue relationship and a sane book on each leg:
    /// Kalshi 0.03/0.04, PM-US 0.08/0.09. Shared by both directions below,
    /// because what they must prove is that the SAME books anchor either lead
    /// at the other leg's touch.
    fn fixture() -> (Rel, BookBuilder) {
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
        (rel, books)
    }

    /// The whole take-take execution design rests on ONE assumption: that a
    /// leg-1 sell on PM-US derives a hedge anchor pointing at the Kalshi leg's
    /// ASK — i.e. leg 2 BUYS Kalshi, completing the K->PM basket.
    ///
    /// `detect_only` is forced on whenever the order path is unarmed, so the
    /// fire path cannot be exercised end-to-end without real money. This pins
    /// the assumption directly instead.
    #[test]
    fn leg1_sell_on_pmus_anchors_a_kalshi_buy() {
        let (rel, books) = fixture();
        // leg 1 is an ASK-side order on the PM-US market (we sell PM YES)
        let a = hedge_anchor(&rel, "P", BookSide::Ask, &books).expect("anchor on the other leg");
        assert_eq!(a.venue, Venue::Kalshi, "hedge must be the OTHER leg");
        assert_eq!(a.market_id, "K");
        assert_eq!(a.side, BookSide::Ask, "we take Kalshi's ask, i.e. we BUY");
        assert_eq!(a.price, "0.04", "at the Kalshi ask the crossing was priced against");
        // and the engine turns an 'ask' anchor into a bid-side (buy) order.
        // Asked of the real `taking_side` rather than of a copy of its rule
        // written out here — a copy is exactly how the mint path and the retry
        // path could come to disagree about which way a hedge trades.
        assert_eq!(super::hedge::taking_side(a.side), BookSide::Bid, "leg 2 must BUY Kalshi");
    }

    /// The MIRROR, which nothing pinned until leg 1 could lead on either
    /// venue (`Candidate::lead`, the 2026-07-29 pulled-1-lot incident): a
    /// leg-1 BUY on Kalshi must derive a hedge anchor pointing at the PM-US
    /// leg's BID — i.e. leg 2 SELLS PM-US YES, which opens the NO, completing
    /// the same K->PM basket from the other end.
    ///
    /// It holds because `hedge_anchor` is symmetric by construction: it finds
    /// the placed leg's index, takes `1 - i`, and reads the SAME side of that
    /// leg's book. Nothing in it names a venue. This test is what keeps that
    /// true — an anchor that pointed at PM-US's ASK would make leg 2 buy the
    /// leg it was meant to sell, doubling the position instead of hedging it.
    #[test]
    fn leg1_buy_on_kalshi_anchors_a_pmus_sell() {
        let (rel, books) = fixture();
        // leg 1 is a BID-side order on the Kalshi market (we buy Kalshi YES
        // at its ask, marketable) — `Candidate::leg1`'s other arm.
        let a = hedge_anchor(&rel, "K", BookSide::Bid, &books).expect("anchor on the other leg");
        assert_eq!(a.venue, Venue::PolymarketUs, "hedge must be the OTHER leg");
        assert_eq!(a.market_id, "P");
        assert_eq!(a.side, BookSide::Bid, "we take PM-US's bid, i.e. we SELL");
        assert_eq!(a.price, "0.08", "at the PM-US bid the crossing was priced against");
        // and a 'bid' anchor becomes an ask-side (sell) order, which is what
        // `arb_venue::wire` sends PM-US as ORDER_INTENT_BUY_SHORT.
        assert_eq!(super::hedge::taking_side(a.side), BookSide::Ask, "leg 2 must SELL PM-US");
    }
}

/// The pull WIRING, driven through the real `run()` loop: what the engine
/// actually does to the executors when its feed goes away or its marks go
/// stale. The audit found the suite covered the pure half of the money path and
/// almost none of the concurrent half; these four are that half.
#[cfg(test)]
mod feed_wiring_tests {
    use super::*;
    // The tick's DECISION lives in `feed_health`; what the engine does with it
    // is here, which is why these tests are here.
    use crate::engine::feed_health::feed_tick;
    use arb_core::scan::{RelLeg, RelType};
    // Feed lines, not intents: these fixtures are what the engine READS.
    use serde_json::json;

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
            // Take-take now prices its net edge against the hedge's slip
            // budget, so a take-take run needs the policy the real config
            // always pairs it with — `run_cfg` gates both on `!bench`.
            hedge_retry: take_take.is_some().then(|| HedgeRetry {
                interval_s: 5.0,
                max_slip: "0.01".into(),
                alarm_after_s: 60.0,
            }),
            take_take,
            armed: false,
            hedges_undischarged: 0,
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
            recovered: AtomicU64::new(0),
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
    /// (message, then closed), which is what makes this deterministic — and
    /// every caller stays well under `FEED_BUDGET`, which is what keeps that
    /// true now that a long enough backlog deliberately does yield to them.
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

    /// The slip gate, WIRED — and countable. `detect` builds a precise
    /// `Skip::NetUnderSlip`, and the loop that consumes it drops every other
    /// `Err` on the floor, so without a counter the shadow run whose entire
    /// job is to measure before money is risked cannot tell "no crossing
    /// existed" from "a crossing was refused by the new gate". That is the
    /// difference between believing the gate only refuses negative-EV trades
    /// and being able to show it.
    #[tokio::test]
    async fn a_crossing_inside_the_slip_budget_is_refused_and_counted() {
        let ts = 1_785_211_200.0; // 2026-07-28T00:00:00Z
        let marks = scratch("marks-slip.json");
        std::fs::write(&marks, marks_at(wall_now())).unwrap();

        // Kalshi ask 0.03 / PM-US bid 0.06 at clip 5: edge 0.03, real fee
        // 0.007384/ct so the 0.02 floor binds, net EXACTLY the 0.01 budget.
        let out = scratch("slip-intents.jsonl");
        let feed = [
            snapshot("kalshi", "K", "0.02", "0.03", ts),
            snapshot("polymarket_us", "P", "0.06", "0.09", ts),
        ];
        let (summary, intents, _) =
            drive(cfg(&out, None, Some(take_take(&marks))), &out, &feed).await;
        assert_eq!(summary["take_take_net_under_slip"], json!(1), "{summary}");
        assert_eq!(summary["take_take_found"], json!(0), "{summary}");
        assert!(!intents.contains(r#""tag":"take-take""#), "{intents}");

        // ...and the control: 2c more edge, nowhere near the budget, still
        // fires and does NOT touch the counter. A gate that refused everything
        // would pass the assertions above on its own.
        let out_ok = scratch("slip-control-intents.jsonl");
        let feed_ok = [
            snapshot("kalshi", "K", "0.02", "0.03", ts),
            snapshot("polymarket_us", "P", "0.08", "0.09", ts),
        ];
        let (sum_ok, int_ok, _) =
            drive(cfg(&out_ok, None, Some(take_take(&marks))), &out_ok, &feed_ok).await;
        assert_eq!(sum_ok["take_take_net_under_slip"], json!(0), "{sum_ok}");
        assert!(int_ok.contains(r#""tag":"take-take""#), "{int_ok}");
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

    /// THE deadline-starvation regression, and the reason `drive` above can
    /// promise no timer arm fires: a pre-filled channel whose sender is dropped
    /// is ready at EVERY poll — message, then closed — so under `biased` alone
    /// the kill switch is never even looked at. That is not a test artifact.
    /// Live on 2026-07-29 the armed engine reported `chan_high_water: 1036` and
    /// a `decision_latency` max of 6_496_952_349 ns: 6.5 seconds of backlog in
    /// which `data/KILL` — documented as a 1-second watch — was never stat'ed.
    ///
    /// The kill file is in place before the first event, so the ONLY question
    /// the assert asks is whether the arm is ever polled. The backlog is many
    /// times `FEED_BUDGET` because a `tokio` interval's first tick is not ready
    /// the instant it is created — it becomes ready once the time driver has
    /// run, which is a few hundred events and a few milliseconds in — so a
    /// backlog of exactly one budget would prove nothing either way.
    #[tokio::test]
    async fn a_feed_backlog_cannot_starve_the_kill_switch() {
        const BACKLOG: usize = 128 * FEED_BUDGET;
        let kill = scratch("starving-KILL");
        std::fs::write(&kill, "halt").unwrap();
        let out = scratch("starving-intents.jsonl");
        let mut cfg = cfg(&out, None, None);
        cfg.kill_file = kill.to_string_lossy().into_owned();

        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(BACKLOG);
        for _ in 0..BACKLOG {
            tx.try_send(FeedMsg {
                line: snapshot("kalshi", "K", "0.03", "0.04", 1_785_211_200.0),
                t_read: std::time::Instant::now(),
            })
            .expect("test channel");
        }
        drop(tx);
        let (txs, _rxs) = executors();
        let summary = run(quoters, by_market, rx, txs, stats(), cfg).await;

        assert_eq!(summary["events"], json!(BACKLOG), "the whole backlog drained");
        assert_eq!(
            summary["chan_high_water"],
            json!(BACKLOG - 1),
            "the depth behind the FIRST event: the feed arm was ready at every \
             poll from here to the close, which is the starving shape: {summary}"
        );
        assert_eq!(
            summary["killed"],
            json!(true),
            "a feed backlog must not be able to hide the halt: {summary}"
        );
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
