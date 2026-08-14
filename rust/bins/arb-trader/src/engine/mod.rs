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
/// `pub(crate)` for ONE item: `note_sidecar_order`. The two modules that place
/// orders without going through `Engine::dispatch` — `maker_exit` and
/// `positions` — have to name the venue ids they got back, or their fills land
/// on this engine's must-stay-0 unexplained-money gauge.
pub(crate) mod fill;
/// `pub(crate)` for ONE item: `venue_reopen_park`, the halt backoff. The
/// venue-truth naked-leg completer (`positions::Act`) parks on the same policy,
/// and a second copy of a backoff curve is a second thing to keep in step.
pub(crate) mod hedge;

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
use std::collections::{BTreeMap, HashMap};
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
    /// Research toxicity feed to RE-READ; None = the gate is off. `Toxgate`
    /// carries one `ts` fixed at load, so a gate nothing reloads goes
    /// permanently open two minutes after startup — see `tox_tick`.
    pub toxgate_file: Option<String>,
    /// Maker APR hurdle inputs to RE-APPLY; None = never (bench/replay, which
    /// have no utilization to float on and a digest that cannot move).
    pub apr: Option<AprCfg>,
    /// The (bar, day) `install_policy` already put on the quoters. Carried
    /// separately from `apr` because a bench run has a hurdle it will never
    /// refresh, and the gauge must still report the bar actually in force.
    pub apr_installed: (f64, String),
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
    /// Opportunistic-unwind policy. `None` = the scan never runs, which is the
    /// default: this is a new strategy on real money, not a defect fix.
    pub unwind: Option<Unwind>,
    /// In-process position marking (`crate::marks`). `None` = off, and off is
    /// the default: with no path given this engine writes no marks file and
    /// behaves exactly as it did before.
    pub marks_out: Option<MarksOut>,
    /// The operator's STATIC `--suppress` declaration, carried so
    /// `maker_exit_tick` can add the exit's own (market, Ask) to it without
    /// dropping it. `Quoter::set_suppress` replaces wholesale, so a tick that
    /// installed only the exit's pair would silently revoke every side another
    /// order-owner had declared.
    pub suppress: std::collections::HashSet<(String, arb_core::model::BookSide)>,
    /// Publish the hurdle, the cap, the PM-US book and the yielded Kalshi asks
    /// for `crate::maker_exit`. `false` = never published, which is how that
    /// module's fail-closed read keeps it inert. Off in bench/replay.
    pub maker_exit_view: bool,
    /// Whether the venue order path is live. Reported in the stats `mode`.
    ///
    /// This existed only as an inference before (`ledger_path.is_some()`), so
    /// every armed run still reported `mode: "shadow"` — the dashboard and any
    /// log-based monitor would say NOT TRADING while it placed real orders.
    pub armed: bool,
    /// Contracts this unit's PREVIOUS run owed a hedge for and never booked,
    /// counted at startup by `orphan::undischarged`. Carried to be REPORTED:
    /// the standing gauge is the half of that census a monitor sees, and it is
    /// what makes `hedges_pending: 0` honest after a restart that forgot an
    /// obligation (2026-07-29 01:34). The engine never HEDGES it — `orphan`
    /// documents why the second hedger would be a double hedge.
    ///
    /// It is no longer the only thing done with the census: those contracts are
    /// also seeded into `risk` at startup (`seed_exposure_from_census`), because
    /// a leg that is real at the venue is exposure whatever the ledger says.
    /// While this field WAS the only consumer, a restart re-authorised the full
    /// per-relationship cap on top of an unhedged position.
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

/// Baskets whose remaining forward APR no longer clears the hurdle the capital
/// they lock would face if it were freed (`crate::unwind`).
///
/// DETECT ONLY, and there is no other mode to configure: the decision is what
/// this ships, and nothing in this workspace can act on it yet. See the module
/// header for why an exit is not expressible as an `Intent` today.
#[derive(Clone)]
pub struct Unwind {
    /// `data/exec/marks.json` — the SAME file the take-take bar is derived
    /// from, and the same staleness rule applies to it. It is read a SECOND
    /// time here, one call after `stats_tick`'s, so across a two-minute rewrite
    /// the two reads can briefly be adjacent versions of the file. Neither can
    /// act on a stale one, which is the property that matters.
    pub marks_path: String,
    /// The process's `--rel-prefix` scope. It does not filter the scan — it
    /// annotates each candidate with whether THIS engine holds a quoter for it,
    /// because today the candidates and the owned families are disjoint sets
    /// and a report that hid that would be a work queue of things that cannot
    /// be worked. See `crate::unwind` §1.
    pub owned_prefixes: Vec<String>,
}

/// Mark open baskets to the live book and rewrite `marks.json` here
/// (`crate::marks`), instead of `arbbot-marks.timer` doing it every 2 minutes
/// off two REST round trips.
///
/// The re-mark TRIGGER is a book event on a market an open basket has a leg on
/// — measured at ~1.15/s across the 14 markets the 2026-07-31 book holds, of
/// which ~1.08/s is PM-US. The write is COALESCED to at most one per
/// [`MarksOut::min_interval_s`], because a 15 KB rewrite at book-event rate is
/// pure I/O on a box whose recorder feeds an armed trader; and it happens at
/// LEAST once per [`MarksOut::max_idle_s`], because `generated_at` is what
/// `taketake::MAX_MARKS_AGE_S` ages, and a book that goes quiet overnight must
/// not age its own bar out.
///
/// It rides a deadline rather than the feed arm for the reason stated at the
/// top of this file: time-based behavior runs on deadlines, never on per-event
/// syscalls. The feed arm only sets a bit.
#[derive(Clone)]
pub struct MarksOut {
    /// Where to write. Pointing this at the SAME file `--marks` reads makes the
    /// engine derive its own take-take bar from marks it wrote — which is the
    /// end state after `arbbot-marks.timer` retires, and a change of TRADING
    /// behaviour, so it is an operator's explicit act and never a default.
    ///
    /// It does not create a price feedback loop: `taketake::blended_apr` reads
    /// only `cost_usd`, `locked_profit_usd` and `resolves_by`, none of which is
    /// marked to market. It does make the staleness guard self-referential —
    /// see `marks_tick`.
    pub out_path: String,
    /// The append-only trade ledger to fold open baskets out of. Re-read when
    /// its (len, mtime) changes, so a basket booked by this engine or appended
    /// by another writer shows up without a restart.
    pub ledger_path: String,
    /// Floor between writes, seconds.
    pub min_interval_s: f64,
    /// Ceiling between writes, seconds — the heartbeat that keeps
    /// `generated_at` fresh when no marked market has ticked.
    pub max_idle_s: f64,
}

/// What the maker APR hurdle is sized from, kept so it can be re-sized.
///
/// BOTH of its terms drift, and both drift in the direction that makes a
/// frozen bar too permissive: utilization rises as baskets book, and the hold
/// shortens every day. Python re-derived this on a timer
/// (`exec/main.py:_tt_refresh`); a value computed once at startup and never
/// revisited is the exact shape of the toxgate defect this same change fixes.
#[derive(Clone)]
pub struct AprCfg {
    /// Explicit `--min-apr`, or None to float with capital utilization.
    pub min_apr: Option<f64>,
    /// Explicit `--apr-asof`, or None for "today", re-derived each refresh so
    /// the hurdle follows the calendar across a multi-day run.
    pub asof: Option<String>,
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

/// What one unwind report was ABOUT: the candidate set as sorted
/// `(rel_id, opened_ts)` (`unwind::identity_set`) AND the skip breakdown's five
/// counts (`unwind::SkipTally::counts`). Two scans that compare equal here are
/// the same finding, and the second one is not worth printing.
///
/// BOTH HALVES, because on the live book the first half is EMPTY: no candidate
/// selects at either floor, so a trigger keyed on candidates alone is a trigger
/// that never fires. See `Engine::unwind_seen`.
type UnwindReport = (Vec<(String, u64)>, (usize, usize, usize, usize, usize));

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
    /// DEQUEUE to the end of `on_feed_line` — the engine actually deciding,
    /// with the channel wait taken out into `queue_wait` below.
    ///
    /// CUMULATIVE over the process, like `queue_wait` and unlike `tick` /
    /// `slowest_tick`, which are windowed. That asymmetry is deliberate (see
    /// `stats_tick`) and it is the reason the two windowed metrics are EMITTED
    /// under `_window` names: side by side in one stats line, a lifetime max
    /// and a 60-second max are indistinguishable, and reading the first as the
    /// second is exactly the error this whole ticket was about.
    decision: Hist,
    /// Producer stamp to DEQUEUE — how long the message sat in the channel
    /// before the loop reached it. Split out of `decision` because `t_read` is
    /// stamped by the PRODUCER, in `feed`'s `tx.send(..).await`, not here:
    /// every sample of `decision` used to carry this wait inside it, so a
    /// quiet engine behind a backlog and a genuinely slow handler produced the
    /// same number.
    ///
    /// This is where the armed engine's ~3s `decision_latency` max always
    /// lived. `spawn_feed` starts stamping at main.rs:1481 and `engine::run`
    /// only starts draining at main.rs:1551 — with `arm_venues` and
    /// `startup_sweep().await`, a live venue round-trip, in between. The
    /// recorder's connect burst is therefore stamped, in full, before the
    /// decision loop exists, and the whole of startup is charged to it. That
    /// artifact is RETIRED: it is `queue_wait` now, and it is explained.
    ///
    /// **READ IT WITH `chan_high_water`, NEVER ON ITS OWN.** `queue_wait`
    /// separates the wait from the decision. It does NOT separate a deep queue
    /// from a loop that stopped being scheduled — both put the whole pause
    /// into the wait, and neither is distinguishable from the other by the
    /// number alone. The depth against the arrival rate is what discriminates:
    /// if only the CONSUMER stalls for T seconds while the producer keeps
    /// running at R events/s, the resuming dequeue sees ~T*R queued and
    /// `chan_high_water` must rise by that much. If it does not, the producer
    /// stopped too.
    ///
    /// That test has already been run, and it came back negative. The dry-run
    /// unit `arbbot-trader-rs` — which has no startup cohort at all, its first
    /// stats line being `elapsed_s: 0.0, book_events: 0, max_ns: 0,
    /// chan_high_water: 0` — took mid-run maxima of 5.55s, 6.50s and 9.04s
    /// across 2026-07-28/29 at a steady ~250 events/s, and `chan_high_water`
    /// did not move for any of the three. It sat at 1036 from 20:40 on the
    /// 28th, through the 6.50s max two hours later, through the 9.04s max at
    /// 14:10 on the 29th, and to the end of the run. A consumer-only stall of
    /// 9.04s at 247/s would have queued ~2200. At that run's `p50_ns: 24576` a
    /// 1036-deep queue drains in ~25ms — it cannot hold a message for nine
    /// seconds. So the producer paused too: these are whole-process pauses,
    /// not a backlog and not one blocked handler.
    ///
    /// THREE processes, one event. The ARMED unit took its own 6.92s max in the
    /// 60s window ending 2026-07-28T22:38:05, strictly inside the 300s window
    /// ending 22:40:39 in which the dry-run took its 6.50s. And the Python
    /// recorder — a separate process, which writes `data/health.jsonl` on a
    /// fixed 1s tick with sub-ms drift — wrote nothing at all between
    /// 22:37:26.687 and 22:37:33.977: a 7.29s gap, inside the armed unit's
    /// window, of the same magnitude as both traders' maxima. Whatever this is,
    /// it is not in this binary.
    ///
    /// It is also NOT RARE, which is what stops it being a smoking gun: across
    /// the same 7.2-hour armed run the recorder's heartbeat has 23 gaps over
    /// 3s, the largest 17.58s. One of them landing in any given minute is a
    /// ~5% event, so the co-occurrence is suggestive, not decisive. It is not
    /// the recorder's own 30s snapshot rebroadcast either: that fires every 30
    /// iterations of the very loop that writes the heartbeat, and the gaps are
    /// spread evenly across that cycle (their line indices mod 30 cover 0..29)
    /// instead of clustering on it. So the recorder's stalls are unexplained
    /// too. That is a WIDER statement of this open incident, not a second one.
    ///
    /// The catch-up fits too. In the minute AFTER its 6.92s max the armed unit
    /// processed 16638 events (277/s) against 13797 and 14234 (230/s, 237/s) in
    /// the two minutes before — yet `chan_high_water` sat flat at 1905 across
    /// all five minutes. A backlog held in the KERNEL socket buffer rather than
    /// in the mpsc looks exactly like that: this counter cannot see a byte until
    /// `socket_feed` has read it, and a pause that stops the reading task is
    /// what would put the backlog there. That is a HYPOTHESIS consistent with
    /// the numbers, not a mechanism anyone has demonstrated — recorded so that a
    /// flat high-water beside a multi-second wait reads as PREDICTED by it,
    /// rather than as evidence that nothing happened.
    ///
    /// **Their cause is UNKNOWN and the incident is OPEN.** Nothing in this
    /// file explains them and nothing here would survive one. A future
    /// `queue_wait: {max_ns: 9e9}` alongside a flat `chan_high_water` is that
    /// same unexplained event — it is NOT the retired startup artifact above,
    /// and it is not "backlog, the benign one". Do not close it as either.
    queue_wait: Hist,
    /// Wall time inside ONE non-feed select arm, SINCE THE LAST STATS LINE.
    /// `decision`/`queue_wait` only describe the feed arm, so a timer handler
    /// that blocked the loop would show up as everyone else's queue wait and
    /// name nobody. Windowed rather than cumulative — see `stats_tick` — and
    /// emitted as `tick_latency_window` so the line says which it is.
    tick: Hist,
    /// The arm that set `tick`'s maximum in the current window, and its time.
    /// A histogram says a tick took 6s; this says which one. Emitted as
    /// `slowest_tick_window`.
    ///
    /// On a healthy line this will usually name `"stats"`, and that is correct,
    /// not a bug to be filtered: `stats_tick` resets the window and only THEN
    /// does `record_tick("stats", ..)` charge its own cost — the `println!` and
    /// flush that printed the previous line — to the window that just opened.
    /// So the stats arm is typically the only arm to have run at all so far.
    /// `"stats"` here means "nothing else was slow"; a line naming `health`,
    /// `tox` or `apr` is the one worth reading.
    slowest_tick: (&'static str, u64),
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
    /// Halt sweeps this engine owes a venue, by venue — retried on the kill
    /// watch until a venue PROVES the book empty.
    ///
    /// This is the one command that reaches orders the engine holds no id for
    /// and the only one that proves the outcome, and it used to be offered
    /// exactly ONCE per halt, after `killed`/`feed_reason` had already latched.
    /// Both halts are entered on an edge (`kill_now && !self.killed`,
    /// `was.is_none()`), so nothing ever re-offered it. There are two ways to
    /// lose it and both were open:
    ///
    ///   * `try_send` LOSES the command when the executor's channel is full.
    ///     Closed 2026-07-29, and never once observed.
    ///   * the executor TAKES it and the VENUE refuses it — `KILL SWEEP FAILED`
    ///     — which is the half that happens. A four-minute DNS outage on
    ///     2026-07-29 failed it on both venues inside thirty seconds of the
    ///     pull, and the engine then sat `feed_pulled: true` over an unproven
    ///     book for the rest of it. Discharging on the QUEUE is what made that
    ///     invisible: `exec_dropped` moves only on a refusal, so the accepted
    ///     sweep left one `eprintln!` and no state at all.
    ///
    /// So the entry survives being queued and is retired by `on_sweep_result`
    /// and nothing else. `cancel.rs` reached the same rule for a per-order
    /// cancel by the same route: an obligation the engine forgot is not an
    /// obligation discharged.
    sweeps_owed: BTreeMap<Venue, OwedSweep>,
    /// Feed-health pull (card 0a7e5478). Holds the REASON, not just a flag, so
    /// a pulled engine can always say why it is silent. Starts pulled when the
    /// check is on: we have not yet proven the feeds are healthy, and the first
    /// tick either clears it or names the problem.
    feed_reason: Option<String>,
    /// Why the toxicity feed cannot be scored against, when it cannot.
    ///
    /// Held as a REASON for the same purpose `feed_reason` is: an engine that
    /// has stopped quoting something must be able to say what stopped it. It
    /// does NOT gate the quote path the way `feed_reason` does — the refusal
    /// is per (market, side) inside the quoter, because this feed covers only
    /// 35 of the 80 legs in the book and a blanket pull would withhold the
    /// other 45 for nothing. This is the operator-visible half: one line on
    /// the edge, and a standing `toxgate_stale` gauge.
    tox_reason: Option<String>,
    /// Last APR bar and as-of day logged, so a refresh that changes nothing
    /// says nothing. Reported in the summary: the bar in force was invisible.
    apr_bar: f64,
    apr_asof: String,
    /// Last unwind scan: how many baskets it would exit, and the contracts
    /// they would free. Both are gauges rather than counters — they describe
    /// the book as of the last scan, so they come DOWN as positions converge.
    n_unwind: usize,
    n_unwind_ct: i64,
    /// ...of which THIS process holds a quoter for, and ...of which clear the
    /// exit floor by less than one tick. Both are the difference between a
    /// finding and a work queue: today the first is 0 on the live book and the
    /// second is how much of the rest is inside its own noise.
    n_unwind_actionable: usize,
    n_unwind_near_floor: usize,
    /// The near-miss population: baskets that cleared the APR test and failed
    /// ONLY the exit floor, and what forcing them out at today's marks would
    /// cost. Without this every one of `crate::unwind::Skip`'s five causes
    /// renders as `unwind_candidates: 0`, which is five different findings
    /// wearing one number.
    n_unwind_near_miss: usize,
    unwind_near_miss_usd: f64,
    /// What the last unwind report was ABOUT, so an unchanged book says nothing
    /// twice. `None` = nothing has been reported yet.
    ///
    /// IT USED TO BE A BARE `Vec` OF THE CANDIDATE SET, AND THAT SILENCED THE
    /// WHOLE FEATURE ON TODAY'S BOOK. `unwind::identity_set(&[])` is `[]`, and
    /// the field started `Vec::new()` — so on a book that selects no candidate
    /// (which the live marks have not, at either floor) the trigger compared
    /// EQUAL on the first tick and on every tick after, and the skip breakdown
    /// never printed at all. A scan whose entire book flipped from
    /// hold-is-better to unpriceable emitted one startup banner and then
    /// nothing, forever. `Option` is what makes the first scan speak, and the
    /// skip counts are what make a change of CAUSE a change.
    ///
    /// The tally's counts and not the tally: `exit_unprofitable_usd` is a price
    /// that moves every tick (`SkipTally::counts`). The candidate half is
    /// SORTED, because `select`'s display order ties on both its keys for
    /// identically-sized baskets on one relationship and a reorder is not a
    /// change.
    /// When each Kalshi ask was FIRST yielded to `crate::maker_exit`. Not
    /// re-stamped while the request stands, because the reader's settle window
    /// measures how long the quoter has been out of the side.
    maker_exit_suppressed: std::collections::BTreeMap<String, std::time::Instant>,
    unwind_seen: Option<UnwindReport>,
    /// Why the last scan refused to decide, when it did. Same shape and same
    /// reason as `tox_reason`: a subsystem that has gone quiet must be able to
    /// say what silenced it.
    unwind_refused: Option<String>,
    /// A marked market has moved since the last mark was written.
    ///
    /// The ONLY thing the feed arm does for marking, and deliberately so: a set
    /// lookup and a bool store per book event, with the rebuild and the write on
    /// `marks_tick`'s deadline.
    marks_dirty: bool,
    /// Wall time of the last successful marks write. Drives both bounds.
    marks_written_at: f64,
    /// The open ledger records the last mark was built from, and the
    /// `(len, mtime_ns)` of the file they came from. Re-read on change rather
    /// than per write: the ledger is ~200 lines and grows by a handful a day,
    /// but re-parsing it every second for nothing is still a syscall and an
    /// allocation the marked markets never asked for.
    marks_records: Vec<serde_json::Value>,
    marks_ledger_stamp: Option<(u64, i64)>,
    /// The markets an open basket has a leg on, by venue — what makes a book
    /// event a re-mark trigger. Rebuilt with `marks_records`.
    marks_watch: HashMap<Venue, std::collections::HashSet<String>>,
    /// Marks files written, and why the last write failed if one did.
    ///
    /// The failure is held as a REASON for the same purpose `tox_reason` and
    /// `unwind_refused` are: this file is an input to the engine's own take-take
    /// bar, so a marking path that has silently stopped writing is the exact
    /// shape of the 2026-07-28 incident. A monitor reads the summary JSON, not
    /// stderr.
    n_marks: u64,
    marks_error: Option<String>,
    /// Rows the last mark could not price, and the markets that is because of.
    ///
    /// A null row is the honest answer to "no book", and it is also
    /// indistinguishable from every other reason a row goes null, so the two
    /// are reported separately: the count is how much of the book this engine
    /// has stopped marking, and the set is WHY. See
    /// [`crate::marks::Marked::no_book`] — on 2026-07-31 this is 8 rows and two
    /// PM-US markets the recorder does not carry, and it is the thing that
    /// blocks retiring `arbbot-marks.timer`.
    marks_unpriced_rows: usize,
    marks_no_book: std::collections::BTreeSet<String>,
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
    /// Times an obligation was parked because the venue refused the place with
    /// the market halted. Rises WITHOUT `n_naked` rising is the shape to read:
    /// the leg is naked and the venue, not the price, is why.
    n_parked: u64,
    /// Fills that expired unclaimed (money we cannot explain) and hedge fills
    /// beyond what an obligation owed. Both must stay 0.
    n_unattributed: u64,
    n_overhedge: u64,
    n_ack: u64,
    n_fill: u64,
    n_hedge: u64,
}

/// One venue's outstanding halt sweep. See [`Engine::sweeps_owed`].
struct OwedSweep {
    /// The halt that owes it, for the log line.
    why: String,
    /// Offers that cannot succeed on their own — the backoff step. See
    /// [`sweep_backoff`] for which refusals count and which do not.
    attempts: u32,
    /// Kill watches to sit out before offering again, counted from the venue's
    /// ANSWER (see `in_flight`), never from the offer.
    ///
    /// A COUNT of watches rather than a deadline, because the 1 Hz kill watch
    /// is the only thing that ever drives this retry: it needs no second clock,
    /// and it stretches with a backlog rather than firing a burst of catch-up
    /// offers after one (`MissedTickBehavior::Skip`), which is the safe
    /// direction for a command that costs a venue round trip.
    wait_ticks: u32,
    /// A sweep is on this venue's executor channel, or on its wire.
    ///
    /// THE INTERLOCK, and it is load-bearing rather than an optimisation: no
    /// backoff can stand in for it, because NOTHING BOUNDS HOW LONG A SWEEP
    /// RUNS. `SweepPolicy::budget` reads like a 20s cap and is not one — its
    /// guard is `rounds_done > 0 && ...` (`sink.rs`), so round 1 is never
    /// budget-checked, and Kalshi's `cancel_all_open` is 1+N HTTP requests that
    /// consult no clock of their own. At the 15s transport timeout a
    /// slow-but-answering venue with N resting orders spends `15*(1+N)` seconds
    /// inside round 1 alone. The 2026-07-29 sweep that motivated this PR took
    /// THIRTY seconds, not twenty.
    ///
    /// Without this bit a timed retry offers again while sweep #1 is still
    /// running, and every offer succeeds — the channel has 1024 slots. The
    /// executor then drains them serially once the venue recovers: N
    /// account-wide cancel-alls landing AFTER the halt cleared, on quotes the
    /// quoter has just re-rested and still believes in. That is the failure the
    /// retry gate in `kill_tick` exists to prevent, arriving by another door,
    /// and once per attempt instead of once per halt.
    in_flight: bool,
}

/// How many kill watches (1 Hz, so seconds) a halt sweep sits out after the
/// venue answers, or after an offer that can never be taken.
///
/// The ways to lose a sweep cost differently, so they are retried differently.
/// A `try_send` the executor REFUSED because its channel was full never touched
/// a wire, and the channel drains at the venue's rate limit — so that one is
/// re-offered on the very next watch, for free, exactly as it was. The two that
/// need a backoff are the ones where a second is not long enough to change
/// anything:
///
///   * the venue ANSWERED and refused. Re-offering at 1 Hz would buy 240 real
///     account-wide cancel-alls out of the four-minute outage on 2026-07-29, on
///     a shared, rate-limited account;
///   * the channel is CLOSED. Nothing will ever drain it, so a 1 Hz retry is a
///     log line a second and no sweep at all.
///
/// The floor is 30 watches for the SHARED ACCOUNT, not to outlast a sweep —
/// `OwedSweep::in_flight` is what does that, because the sweep has no
/// enforceable duration to outlast. It doubles to a 120 ceiling so a venue that
/// keeps refusing costs one attempt every two minutes instead of 3,600, and so
/// a venue that recovers is swept within two minutes of doing so.
///
/// Kalshi's `premise_broken` (`Book::premise_broken`, mid-session only) is the
/// case that can never succeed, and it is the expensive one to be wrong about:
/// the venue is UP, so nothing spends the budget and all 4 rounds run, which is
/// 4 x (1 paged `cancel_all_open` + 3 `resting_order_ids` reads) = 16 paged
/// account reads per attempt. At the ceiling that is 16 every two minutes, on
/// the same key `arbbot-hedge.timer` trades under — costly, bounded, and it
/// settles there rather than spinning.
fn sweep_backoff(attempts: u32) -> u32 {
    const FLOOR: u32 = 30;
    const CEILING: u32 = 120;
    let doubled = 1u32.checked_shl(attempts.saturating_sub(1)).unwrap_or(u32::MAX);
    FLOOR.saturating_mul(doubled).min(CEILING)
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
        if let Some(u) = cfg.unwind.as_ref() {
            eprintln!(
                "[unwind] DETECT ONLY (places nothing) — exits below the maker hurdle, \
                 marks {}",
                u.marks_path
            );
        }
        if let Some(m) = cfg.marks_out.as_ref() {
            eprintln!(
                "[marks] marking to the live book -> {} (ledger {}, at most 1/{:.0}s, \
                 at least 1/{:.0}s){}",
                m.out_path,
                m.ledger_path,
                m.min_interval_s,
                m.max_idle_s,
                // The one configuration that changes what this engine TRADES:
                // the bar it reads back is then one it wrote.
                if cfg.take_take.as_ref().is_some_and(|t| t.marks_path == m.out_path)
                    || cfg.unwind.as_ref().is_some_and(|u| u.marks_path == m.out_path)
                {
                    " — THIS IS THE FILE THIS ENGINE ALSO DECIDES FROM"
                } else {
                    ""
                }
            );
        }
        let feed_reason: Option<String> =
            cfg.health_file.is_some().then(|| "startup — feeds not yet proven healthy".to_string());
        // Same read `install_policy` just made, for the other half of the
        // answer: it decides what to INSTALL on the quoters, this decides
        // whether the engine may quote at all. Read here rather than passed in
        // so that the state and the file cannot drift apart, exactly as
        // `tt_bar` is re-derived from marks above.
        let tox_reason = cfg
            .toxgate_file
            .as_deref()
            .and_then(|p| crate::load_toxgate(p, wall_now()).stale);
        let (apr_bar, apr_asof) = cfg.apr_installed.clone();
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
            queue_wait: Hist::new(),
            tick: Hist::new(),
            slowest_tick: ("none", 0),
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
            sweeps_owed: BTreeMap::new(),
            feed_reason,
            tox_reason,
            apr_bar,
            apr_asof,
            n_unwind: 0,
            n_unwind_ct: 0,
            n_unwind_actionable: 0,
            n_unwind_near_floor: 0,
            n_unwind_near_miss: 0,
            unwind_near_miss_usd: 0.0,
            maker_exit_suppressed: std::collections::BTreeMap::new(),
            unwind_seen: None,
            unwind_refused: None,
            marks_dirty: false,
            // 0.0, not `wall_now()`: the first `marks_tick` must write
            // immediately, so a restart cannot leave the previous writer's file
            // aging while this one waits out a heartbeat.
            marks_written_at: 0.0,
            marks_records: Vec::new(),
            marks_ledger_stamp: None,
            marks_watch: HashMap::new(),
            n_marks: 0,
            marks_error: None,
            marks_unpriced_rows: 0,
            marks_no_book: std::collections::BTreeSet::new(),
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
            n_parked: 0,
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
                // For a hedge RETRY, the attempt it supersedes. Only hedge
                // attempts are in `hedge_orders`, so this is `None` for every
                // other place — and for a first attempt, which supersedes
                // nothing. See `HedgeOrder::supersedes`: without it a lost fill
                // frame turns one hedge into two.
                let supersedes = match &it {
                    Intent::Place(p) => hedge::superseded(
                        &self.hedge_orders,
                        &self.oid_venue,
                        &p.order_id,
                    ),
                    _ => None,
                };
                for action in intent_actions(
                    &it,
                    self.cfg.armed,
                    &self.oid_venue,
                    &mut self.parked_cancels,
                    now,
                    supersedes.clone(),
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
        self.owe_sweeps(why);
        for q in quoters.iter_mut() {
            q.cancel_all(&mut self.cx, self.last_now, &mut self.intents);
            self.drain_intents(Some(&q.rel));
        }
    }

    /// Every venue with an executor owes a sweep from this moment, and it goes
    /// out BEFORE the per-order cancels the halt is about to emit.
    ///
    /// The order is the point. Both go into ONE per-venue channel drained at
    /// `--rate-limit` (8/s/venue live) by a single sequential task that awaits
    /// each venue call inline, so a sweep queued behind C cancels waits out C
    /// of that budget PLUS their round trips — `(C+1)/8`s on an empty token
    /// bucket, `(C+1-8)/8` on a full one, the bucket capping at `rate_per_s`.
    /// Measured over the live intent streams, grouping cancels by the tape `ts`
    /// one `pull_quotes` stamps on all of them:
    ///   * the ARMED engine's 11 pulls to 2026-07-29 emitted 21-23 cancels
    ///     each, 10-12 per venue -> 0.4-1.6s;
    ///   * the full-registry shadow's 47 emitted 29-33 (15-19 per venue) ->
    ///     1.0-2.5s, and ONE of 138 (Kalshi 53, PM-US 83) -> 5.8-10.5s. The
    ///     registry this engine quotes is what decides which of those it is.
    ///
    /// Most of it was spent on work the sweep redoes: PM-US posts one
    /// empty-bodied account-wide cancel-all, and Kalshi pages its order list
    /// and DELETEs every resting order THIS STACK minted
    /// (`arb_venue::gateway::is_ours`), which is every order this engine can
    /// have placed. So every one of those cancels is subsumed.
    ///
    /// ONE STATE BREAKS THAT, and it is worth knowing because it only exists
    /// with the head-of-queue ordering above. When Kalshi's order list stops
    /// echoing `client_order_id`, `cancel_all_open` can attribute nothing and
    /// cancels NOTHING (`KalshiGateway::cancel_all_open`, `Book::premise_broken`)
    /// — while the 10-12 per-order cancels queued behind it use
    /// `CancelBy::VenueId` and would have worked. A kill in that state leaves
    /// quotes live for up to `SweepPolicy::budget` (20s) longer than it would
    /// have before this reordering.
    ///
    /// Accepted rather than special-cased: skipping the head sweep on that
    /// verdict means the engine carrying venue state it currently has no reason
    /// to hold, to optimise a 20s window in a state that has never occurred and
    /// that pages loudly the moment it does (the sweep refuses, and the halt
    /// exits non-zero). Recorded here so the next person to see a slow kill has
    /// the explanation rather than a mystery.
    ///
    /// THE COST, which is real: the sweep now blocks the HEAD of that queue
    /// rather than the tail. It is awaited inline and bounded by
    /// `SweepPolicy::budget` (20s), so the cancels and the one client-id
    /// escalation per tick behind it wait that long in the worst case. Taken
    /// anyway, because the old order was worse in exactly the incident that
    /// matters: each of those 10-12 cancels can burn the transport's 15s
    /// timeout against a venue that has stopped answering, which is minutes
    /// before the one command that PROVES the book empty is even attempted.
    ///
    /// The cancels still go out, because a venue's order list can lag a
    /// just-placed order that round 1 of `cancel_all_open` therefore misses
    /// (`SweepPolicy::rounds`), and because the park is the record that the
    /// cancel is owed. They just no longer go first.
    ///
    /// NOT "every armed venue": `spawn_executors` inserts a `Sender` for all
    /// three venues unconditionally, sink or no sink, and `main` calls it in
    /// every mode — so this marks three on every halt, INTL included, which is
    /// never armed. An executor with no sink counts the command and drops it,
    /// which is what a dry run IS. The empty case is `test_engine`, which holds
    /// no executors at all.
    fn owe_sweeps(&mut self, why: &str) {
        for venue in self.exec_txs.keys() {
            // Overwriting any entry still owed, backoff and all: a NEW halt is
            // new evidence about the book, and it is offered at once rather
            // than waiting out a backoff earned by the previous one.
            self.sweeps_owed.insert(
                *venue,
                OwedSweep {
                    why: why.to_string(),
                    attempts: 0,
                    wait_ticks: 0,
                    // NOT preserved from any entry this overwrites: a sweep
                    // already on the wire cannot prove a book that has changed
                    // since it started, and its answer will clear this anyway.
                    in_flight: false,
                },
            );
        }
        self.sweep_owed_venues();
    }

    /// Offer `SweepAndVerify` to every venue that owes one and is due, and go on
    /// owing it until a venue answers that the book is clean.
    ///
    /// The retry is the whole of it: see [`Engine::sweeps_owed`]. The halt state
    /// still LATCHES on a sweep that never lands — a halt that cannot prove the
    /// book clean must stop quoting all the more — so what is retried is the
    /// sweep alone, on the 1s kill watch.
    ///
    /// Through `dispatch`, not a bare `try_send`, so a lost sweep MOVES
    /// `exec_dropped`. `dispatch` holds the only `dropped.fetch_add` in the
    /// binary and both halts used to send their sweep around it, so
    /// "`exec_dropped` is 0, therefore no sweep has ever been dropped" was
    /// never a statement this process could support — the counter could not
    /// see the command. It can now.
    ///
    /// QUEUEING IS NOT PROOF, and this no longer pretends otherwise. The entry
    /// stays owed across a successful `try_send` and is retired only by
    /// [`Engine::on_sweep_result`], which is the venue's own answer relayed by
    /// the executor that awaited it.
    fn sweep_owed_venues(&mut self) {
        for (venue, mut owed) in std::mem::take(&mut self.sweeps_owed) {
            // A sweep is already queued or running for this venue. Nothing to
            // decide until it answers — and the backoff cannot stand in for
            // this check, because a sweep has no bounded duration to wait out
            // (see `OwedSweep::in_flight`).
            if owed.in_flight {
                self.sweeps_owed.insert(venue, owed);
                continue;
            }
            if owed.wait_ticks > 0 {
                owed.wait_ticks -= 1;
                self.sweeps_owed.insert(venue, owed);
                continue;
            }
            let queued = self.dispatch(venue, Action::SweepAndVerify);
            if !queued {
                // A full channel and a dead executor are the SAME `try_send`
                // failure and the same `false` — only `is_closed` tells them
                // apart. Worth telling apart because they need opposite
                // handling and mean opposite things: one is a backlog that
                // drains, the other is a process that can no longer sweep
                // anything at all (close to unreachable —
                // `install_armed_panic_hook` takes the process down first — but
                // it is a mode the old fire-and-forget did not have).
                let closed = self.exec_txs.get(&venue).is_none_or(|tx| tx.is_closed());
                eprintln!(
                    "[engine] {}: could not queue sweep for {venue:?} — executor {}; book \
                     NOT proven clean, still owed",
                    owed.why,
                    if closed {
                        "GONE, its channel is closed and NO sweep is possible from this \
                         process"
                    } else {
                        "backlogged"
                    }
                );
                if !closed {
                    // Free to re-offer: nothing reached a wire, and the channel
                    // drains. Back onto the next watch, unchanged.
                    self.sweeps_owed.insert(venue, owed);
                    continue;
                }
                // A dead channel changes nothing within a second, and nothing
                // will answer for it either — so it is the one refusal that
                // takes the backoff instead of the interlock.
                owed.attempts += 1;
                owed.wait_ticks = sweep_backoff(owed.attempts);
                self.sweeps_owed.insert(venue, owed);
                continue;
            }
            // An executor took it. The entry is now BLOCKED on the venue's
            // answer, not on a clock: `on_sweep_result` is what unblocks it,
            // and what starts the backoff if the answer is a refusal.
            owed.attempts += 1;
            owed.in_flight = true;
            self.sweeps_owed.insert(venue, owed);
        }
    }

    /// The venue's answer to a halt sweep — the ONLY thing that discharges one,
    /// and the only thing that releases the interlock.
    ///
    /// Both branches clear `in_flight`: an answer means this venue's executor is
    /// free, whichever way it went. A refusal then starts the backoff HERE
    /// rather than at the offer, so the wait is measured from when the venue
    /// actually finished — the sweep it waits out has no bounded duration to
    /// pre-empt (see [`OwedSweep::in_flight`]). Nothing is logged, because the
    /// executor has already said what went wrong in the venue's own words
    /// (`KILL SWEEP FAILED`) at the moment it happened.
    ///
    /// An unparseable or absent `ok` reads as a refusal, which keeps the
    /// obligation owed. That is the direction to fail in: the cost is one extra
    /// sweep, and the alternative is calling a book proven on a field nobody
    /// could read.
    ///
    /// A late answer is not told from a current one, unlike `cancel_result`'s
    /// numbered attempts. Per venue there is ONE executor draining ONE channel
    /// in order, so the only way to get a stale answer is for a halt to re-owe a
    /// venue while a sweep is in flight — and the sweep that re-owed it is
    /// already queued behind the one answering, so the book still gets proven
    /// and the gauge is early rather than wrong.
    fn on_sweep_result(&mut self, v: &serde_json::Value, venue: Venue) {
        if let Some(owed) = self.sweeps_owed.get_mut(&venue) {
            owed.in_flight = false;
            if v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false) {
                self.sweeps_owed.remove(&venue);
            } else {
                owed.wait_ticks = sweep_backoff(owed.attempts);
            }
        }
    }

    /// Re-read the research toxicity feed onto every quoter.
    ///
    /// `Toxgate` carries a single `ts` captured when the file was parsed, and
    /// `Toxgate::verdict` will not score against an opinion past
    /// `TOXGATE_MAX_AGE`. So without this the gate is not merely frozen, it is
    /// a two-minute gate: at startup + 120s every scored side goes `Untrusted`
    /// and stays there. Reloading is what makes the gate a gate.
    ///
    /// The refusal itself is NOT here. It is per (market, side) in the quoter,
    /// where it is scoped to sides this feed actually covers — 35 of the 80
    /// legs in the live book, all of them Kalshi. An engine-level pull would
    /// withhold the other 45, including 38 Polymarket / PM-US legs the model
    /// has never scored and never will. What is here is the operator-visible
    /// half: one line on the edge, and a standing gauge.
    pub(super) fn tox_tick(&mut self, quoters: &mut [Quoter]) {
        let Some(path) = self.cfg.toxgate_file.clone() else { return };
        let load = crate::load_toxgate(&path, wall_now());
        // Install whatever PARSED, stale or not: a stale document is still the
        // coverage map the per-side refusal needs. A read that fails outright
        // leaves the last good document in place for the same reason — the set
        // of sides this model covers is the one thing that does not go stale.
        if let Some(gate) = &load.gate {
            for q in quoters.iter_mut() {
                q.set_toxgate(Some(gate.clone()));
            }
        }
        let reason = load.stale;
        // Edge-triggered, like `feed_reason` and the take-take bar: the reason
        // string carries an age that moves every tick, so comparing the strings
        // would log every tick and a line every tick is a line nobody reads.
        if reason.is_some() != self.tox_reason.is_some() {
            match &reason {
                Some(why) => {
                    eprintln!("[toxgate] UNUSABLE ({why}) — every side it covers is withheld")
                }
                None => eprintln!("[toxgate] feed current again — scored sides quote again"),
            }
        }
        self.tox_reason = reason;
    }

    /// Re-size the maker APR hurdle. Port of `exec/main.py:_tt_refresh`, which
    /// recomputed the bar on a timer and pushed it onto every quoter.
    ///
    /// Both terms drift the same way — utilization RISES as baskets book, and
    /// the hold SHORTENS every day — so a bar computed once at startup decays
    /// toward being too permissive, which is the direction that costs money.
    pub(super) fn apr_tick(&mut self, quoters: &mut [Quoter]) {
        let Some(a) = self.cfg.apr.clone() else { return };
        let (bar, asof, _) =
            crate::apply_apr(quoters, a.min_apr, a.asof.as_deref(), self.cfg.risk.as_deref());
        // 0.05%/yr: below that the quantized cent price cannot move, so it is
        // not a change anyone could act on.
        if (bar - self.apr_bar).abs() >= 0.05 || asof != self.apr_asof {
            eprintln!(
                "[apr] maker hurdle {:.2} -> {bar:.2}%/yr, holds measured from {asof}",
                self.apr_bar
            );
            self.apr_bar = bar;
            self.apr_asof = asof;
        }
    }

    /// Publish what `crate::maker_exit` cannot derive, and install what it asks
    /// for.
    ///
    /// TWO DIRECTIONS, ONE TICK, and they belong together: the exit loop asks
    /// for a Kalshi ask to be yielded, and the only honest answer to "has it
    /// been" is one published by the code that did the yielding. Split across
    /// two ticks they could disagree about whether the entry quoter is out.
    ///
    /// SILENT WHEN HALTED, and that is the whole halt path for the exit loop.
    /// `crate::maker_exit::VIEW_MAX_AGE` refuses a view older than three of
    /// these, so a killed or feed-pulled engine makes every exit decision refuse
    /// and makes the loop PULL what it has resting — without this module having
    /// to reach into it. `unwind_tick` documents the same gate for the same
    /// reason: an exit is still an order.
    pub(super) fn maker_exit_tick(&mut self, quoters: &mut [Quoter]) {
        if !self.cfg.maker_exit_view {
            return;
        }
        if self.killed || self.feed_reason.is_some() {
            return;
        }
        let asked = crate::maker_exit::suppress_requests();
        // Base + asked, never asked alone: `set_suppress` replaces the whole
        // set and the operator's `--suppress` declaration must survive.
        let mut want = self.cfg.suppress.clone();
        for m in &asked {
            want.insert((m.clone(), BookSide::Ask));
        }
        for q in quoters.iter_mut() {
            q.set_suppress(want.clone());
        }
        let now = std::time::Instant::now();
        // FIRST installed, not most recently confirmed: the settle window the
        // reader applies is "how long has the quoter been out of this side",
        // and re-stamping it every tick would make that window never elapse.
        self.maker_exit_suppressed.retain(|m, _| asked.contains(m));
        for m in &asked {
            self.maker_exit_suppressed.entry(m.clone()).or_insert(now);
        }
        // PM-US top-of-book ASK for every market this engine holds a book for.
        // The engine's book is the ONLY PM-US price read in this process —
        // `PmusGateway` has no `market_quote` — so an exit priced without it
        // would be a two-leg trade with one leg valued.
        let mut pm_ask = std::collections::BTreeMap::new();
        for (market, ask) in self.books.pm_us_asks() {
            pm_ask.insert(market, ask);
        }
        crate::maker_exit::publish_view(crate::maker_exit::EngineView {
            apr_bar: self.apr_bar,
            global_cap_usd: self.cfg.risk.as_ref().map_or(0.0, |r| r.global_cap_usd()),
            pm_ask,
            suppressed_at: self.maker_exit_suppressed.clone(),
        });
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
            // NOT a pull: scored sides are withheld, the rest quote on. The
            // bar is reported because "what hurdle is in force right now" was
            // not answerable from anything this process emitted.
            "toxgate_stale": self.tox_reason.is_some(),
            "maker_apr_bar": self.apr_bar,
            // Baskets the unwind scan would exit, and the contracts they hold.
            // GAUGES, not counters: they describe the book as of the last scan.
            // A standing non-zero `unwind_contracts` against a refusing class
            // cap is the capital loop being unable to turn — the engine cannot
            // quote because the capital is committed, and this is how much of
            // it the book is currently offering to give back. 0 while
            // `unwind` is off, which is the default.
            "unwind_candidates": self.n_unwind,
            "unwind_contracts": self.n_unwind_ct,
            // ...of which this process could act on. `unwind_candidates` high
            // and this pinned at 0 is the finding that blocks arming: what the
            // recycler selects and what this engine quotes are disjoint sets
            // (`crate::unwind` §1). It is the FIRST number to read, not a
            // footnote to the one above.
            "unwind_actionable": self.n_unwind_actionable,
            // ...of which clear the exit floor by less than one tick, so one
            // tick of book movement unmakes them.
            "unwind_near_floor": self.n_unwind_near_floor,
            // Baskets that cleared the APR test and failed ONLY the exit floor,
            // and what forcing them out at today's marks would cost (negative).
            // The APR test alone selects most of the book; this is the number
            // that says why selecting on it would be a losing strategy.
            "unwind_near_miss": self.n_unwind_near_miss,
            "unwind_near_miss_usd": self.unwind_near_miss_usd,
            // The maker-exit placer (`--maker-exit` only; 0 forever without it).
            // `maker_exit_unresolved` MUST STAY 0: it counts a Kalshi exit that
            // FILLED and whose PM-US leg did not close, which is a naked short
            // the ledger still records as a hedged basket.
            "maker_exit_placed": crate::maker_exit::placed(),
            "maker_exit_closed": crate::maker_exit::closed(),
            "maker_exit_refused": crate::maker_exit::refused(),
            "maker_exit_unresolved": crate::maker_exit::unresolved(),
            // ...and WHY the scan decided nothing, when it refused to decide at
            // all. `null` = it ran. Every gauge above reads zero on a converged
            // book AND on a degenerate class cap, an off-band hurdle and torn
            // marks — four findings wearing one shape — and the monitors read
            // THIS JSON, not stderr (PR #46). Same contract as `toxgate_stale`,
            // one level more informative: a subsystem that has gone quiet must
            // be able to say what silenced it, in the file that is read.
            "unwind_refused": self.unwind_refused.as_deref(),
            // Marks files written this run, and why the last write failed if it
            // did. 0 while `--marks-out` is off, which is the default.
            //
            // `marks_written` RISING is the health signal — this engine is the
            // input to its own take-take bar when it writes the file the bar is
            // read from, so a flat counter is the 2026-07-28 incident in
            // advance. `marks_error` is `null` when the last write succeeded,
            // and it is here rather than only on stderr for the reason every
            // other reason-string in this block is (PR #46): the monitors read
            // this JSON.
            "marks_written": self.n_marks,
            "marks_error": self.marks_error.as_deref(),
            // Rows the last mark published UNPRICED, and the markets that is
            // because of. Non-zero means this engine is publishing a marks file
            // that covers LESS of the book than `mark_positions.py`'s would —
            // which is a fact about the recorder's universe, not about this
            // code, and the reason both numbers are here rather than in a log
            // line nobody greps.
            "marks_unpriced_rows": self.marks_unpriced_rows,
            "marks_no_book": self.marks_no_book.len(),
            "risk_allowed": self.cfg.risk.as_ref().map(|r| r.stats().0).unwrap_or(0),
            "risk_rejected": self.cfg.risk.as_ref().map(|r| r.stats().1).unwrap_or(0),
            // Contracts held against the caps by quotes that are RESTING and
            // have not filled. The caps refuse on this number, so without it
            // they refuse on invisible state: it should track the resting book
            // and come back down: one that only ever rises is a reservation
            // leak, which ratchets the caps shut and stops all trading.
            "risk_reserved": self.cfg.risk.as_ref().map(|r| r.reserved_ct()).unwrap_or(0.0),
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
            // Venues a halt has told to cancel EVERYTHING and prove it, that
            // no venue has yet proven clean. The per-order obligation above
            // has had a gauge since it had a retry; this one is the same
            // thing for the account-wide command, and it is the only state
            // that can say "this halt never proved its book" — `exec_dropped`
            // moves for a sweep the executor REFUSED and cannot see one it
            // accepted and the venue then failed, which is the case that
            // happens.
            //
            // 0 is the healthy value, and a NON-zero one does not by itself
            // mean anything is wrong right now. It deliberately does NOT come
            // back down when a halt clears over an unproven book (`kill_tick`):
            // the obligation is kept, so a session that recovered fully from
            // this morning's outage would read 2 for the rest of its life. It
            // is a record that a book went unproven, not a live fault — and
            // while it stands, the venues it names are ones this process has
            // not swept, whether because they refused, because their executor
            // is gone, or because no halt has been in force to retry under
            // since. Nothing reads it automatically.
            "sweeps_owed": self.sweeps_owed.len(),
            "hedges_pending": self.pending_hedges.len(),
            // ...of which THIS process knows nothing: contracts a previous run
            // owed a hedge for and never booked. `hedges_pending` counts only
            // what is live in memory, so it read 0 after the 01:34 restart on
            // 2026-07-29 while a PM-US short was still real at the venue. This
            // is the standing signal that it is not the whole story;
            // arbbot-hedge.timer is what completes them (see `orphan`).
            "hedges_undischarged": self.cfg.hedges_undischarged,
            "hedges_retried": self.n_retry,
            // ...which counts retries the engine DECIDED on, and can therefore
            // run ahead of the places that reached a venue: the executor
            // withholds one whose superseded attempt turns out to have filled,
            // or could not be read. That is the discriminant, and it only IS
            // one if it is somewhere a monitor can read it — steady 0 is the
            // resting state.
            "hedge_retries_refused": crate::exec::hedge_retries_refused(),
            "hedges_naked": self.n_naked,
            // ...and obligations parked because the VENUE said the market is
            // halted. Not an alarm on its own — a halt is the venue's business,
            // not a fault of ours — but it is the only place the distinction
            // between "the book will not offer a price" and "there is no book
            // to offer one" is visible without reading the log.
            "hedges_parked": self.n_parked,
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
            // Fractional contracts filled but not yet whole, across orders, in
            // HUNDREDTHS. Kalshi splits a fill across price levels and the
            // pieces are fractional, so this is normally a transient single
            // digit while an order's pieces land. A number that sits high is
            // dust nothing will ever hedge — see `fills::kalshi_fill_dust_hundredths`.
            "kalshi_fill_dust_hundredths": crate::fills::kalshi_fill_dust_hundredths(),
            // Fills the venue had that this process's WS never delivered,
            // recovered by the reconciliation and hedged. This is the gauge
            // that says the defect ACTUALLY happened — `kalshi_fill_gaps`
            // above only says there was a window it could have happened in.
            // Not an error: nonzero is the repair working.
            "kalshi_fills_recovered": crate::fills::kalshi_fills_recovered(),
            // ...and reconciliations that could not run at all: venue refused,
            // background budget spent, unparseable response, or a history
            // longer than the page cap. Must stay 0. While it rises the Kalshi
            // fill totals are a local sum with nothing behind them — the
            // posture the reconciliation exists to end.
            "kalshi_reconcile_failures": crate::fills::kalshi_reconcile_failures(),
            // ...and orders whose venue rows were REFUSED because merging them
            // would have exceeded the venue's own total for the window.
            //
            // NOT a must-stay-0 gauge, unlike the two above. The reconciliation
            // runs immediately after resubscribe — exactly when a post-gap
            // burst is landing — and Kalshi is not read-your-writes, so an
            // order the socket has counted ahead of the REST list is refused
            // and retried. On a busy account that is expected.
            //
            // This number alone cannot say whether the cause is that lag or the
            // one that matters (WS and REST `trade_id` being different id
            // spaces). Nothing derivable from a count can: see
            // `fills::kalshi_reconcile_rejected` for two earlier claims here
            // that were wrong in both directions. The discriminant is the
            // ORDER ID in the `[fills] kalshi reconcile REFUSED` log line —
            // under a mismatch the same id repeats on every reconcile for the
            // life of the process, under lag it merges once REST catches up.
            "kalshi_reconcile_rejected": crate::fills::kalshi_reconcile_rejected(),
            // Naked legs the venue-truth positions reconciliation
            // (`--positions-recon`, OFF by default) has CONFIRMED across two
            // cycles. Every gauge above this one is derived from fills this
            // process saw; this is the only one derived from what the venues
            // actually hold, so it is the only one that can see a leg the
            // engine never attributed — or fail to see one another owner has
            // since closed, which `hedges_naked` cannot (2026-07-30, where it
            // read 1 for a leg arbbot-hedge.timer had already hedged).
            //
            // READ IT WITH `positions_recon_age_s`. A 0 here is "no naked leg
            // in the last snapshot", not "no naked leg", and the difference is
            // exactly how old that snapshot is.
            "positions_recon_naked": crate::positions::naked(),
            // ...and imbalances seen once but not yet by a second cycle. A
            // transient count is the guard working (a venue read that did not
            // reproduce, or a hedge in flight); one that never converts is
            // venue noise being absorbed rather than reported.
            "positions_recon_unconfirmed": crate::positions::unconfirmed(),
            // Cycles abandoned because a read could not be trusted: a venue
            // refusal (PM-US served 503s in runs on 2026-07-31), a spent
            // background budget, or two position reads that would not agree.
            // Not must-stay-0 — refusing IS the safe outcome — but it rising
            // while the age below rises with it means the reconciliation is
            // down and `positions_recon_naked` is a memory.
            "positions_recon_failures": crate::positions::failures(),
            // Seconds since a cycle last COMPLETED; -1 = never, which includes
            // the default-off case. This is the gauge to alarm on: a
            // reconciliation that cannot read a venue holds its last answer
            // forever rather than inventing a clean one, and this is what says
            // so.
            "positions_recon_age_s": crate::positions::age_s(),
            // Naked legs this process COMPLETED with a real order and booked
            // (`--positions-recon-act` only; 0 forever without it). Every one is
            // money spent from a venue-truth read rather than from a fill this
            // engine watched, so it is the count to reconcile against
            // `data/exec/trades.jsonl` by hand while the policy is young.
            "positions_recon_acted": crate::positions::acted(),
            // Confirmed findings the act pass DECLINED. Not an error count:
            // waiting for a book that pays is the policy, and a probe-owned
            // market, an unaccounted-for position and an unprofitable ask all
            // land here. Read it against `positions_recon_naked` — legs
            // confirmed and never acted on are legs a guard is holding, and the
            // journal names which guard for each one.
            "positions_recon_act_refused": crate::positions::act_refused(),
            // MUST STAY 0. Orders the venue ACCEPTED whose fate this process
            // could not read. Not a refusal and not a fill: contracts that may
            // be in the account and are certainly not in the ledger, which is
            // the one state no exposure fold can see. Alarm on any change.
            "positions_recon_act_unresolved": crate::positions::act_unresolved(),
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
            // Four latency metrics on one line, and TWO DIFFERENT WINDOWS. The
            // `_window` suffix is the only thing that says so, and it is on the
            // emitted key rather than only in a doc comment because the key is
            // what an operator reads at 3am. Without it, a `decision_latency`
            // max pinned by startup hours ago sits beside a 60-second
            // `tick_latency` max looking like a live 6000x discrepancy — which
            // is a new way to make the exact misreading this ticket existed to
            // kill. Unsuffixed = cumulative over the process.
            "decision_latency": self.decision.summary(),
            "queue_wait": self.queue_wait.summary(),
            "tick_latency_window": self.tick.summary(),
            "slowest_tick_window": {"arm": self.slowest_tick.0, "ns": self.slowest_tick.1},
            "exec_hop_latency": self.exec_stats.hop.summary(),
            "elapsed_s": (elapsed * 10.0).round() / 10.0,
            "eps": if elapsed > 0.0 { (self.n_ev as f64 / elapsed) as u64 } else { 0 },
        })
    }

    /// Time one non-feed select arm, and remember the worst of the window by
    /// NAME.
    ///
    /// `decision`/`queue_wait` describe the feed arm only. A timer handler that
    /// blocked the loop would spread itself over the queue wait of every
    /// message behind it and name nobody — which is precisely the reading
    /// `decision_latency` alone could never give.
    fn record_tick(&mut self, arm: &'static str, t: std::time::Instant) {
        let ns = t.elapsed().as_nanos() as u64;
        self.tick.record(ns);
        // The name tracks the window's maximum, not its most recent sample.
        if ns > self.slowest_tick.1 {
            self.slowest_tick = (arm, ns);
        }
    }

    /// One line off the feed channel. `queued` is the channel depth behind it.
    ///
    /// The two clocks are split HERE, around one call, rather than at each
    /// handler's last statement — so that the thirteen paths through
    /// `on_feed_line` that return early are measured too. The one that does the
    /// most work is `FEED_DOWN`: it pulls every quote and ENQUEUES a cancel per
    /// resting order — 23 of them on the 2026-07-28T20:13:06 disconnect.
    ///
    /// Enqueues, and no more. `pull_quotes` writes an intent line and
    /// `try_send`s; nothing on this path awaits a venue, so what this histogram
    /// charges the handler is microseconds. The ~1.0s that disconnect took to
    /// clear belongs to the EXECUTOR, which drains ONE per-venue channel at
    /// `--rate-limit` (8/s/venue) awaiting each call inline — 23 cancels at
    /// 11-12 per venue is ~1.4s by the arithmetic in `owe_sweeps`, and the
    /// journal splits it cleanly: FEED DOWN logged 20:13:06.983350, first
    /// cancel completed 97ms later at 20:13:07.080657, last at 20:13:08.113269.
    ///
    /// So a post-PR `decision_latency: {max_ns: 1e9}` is NOT "a FEED_DOWN,
    /// known". This handler cannot spend a second; if the metric says it did,
    /// something else did it. Attributing a stall is the whole job of the
    /// number — do not hand it a pre-cooked excuse.
    fn on_feed(
        &mut self,
        m: FeedMsg,
        queued: usize,
        quoters: &mut [Quoter],
        by_market: &ByMarket,
    ) {
        self.n_ev += 1;
        self.chan_hw = self.chan_hw.max(queued);
        // THE clock this metric was missing. `m.t_read` is the PRODUCER's stamp
        // (`feed`'s `tx.send(..).await`), so everything before this line is time
        // the message spent in the channel and everything after it is this
        // engine actually deciding. Recorded apart because summing them is what
        // made a blocked handler and a quiet feed behind a backlog the same
        // number.
        self.queue_wait.record(m.t_read.elapsed().as_nanos() as u64);
        let t_deq = std::time::Instant::now();
        self.on_feed_line(&m.line, quoters, by_market);
        self.decision.record(t_deq.elapsed().as_nanos() as u64);
    }

    /// Route one feed line to its handler. Split out of `on_feed` only so that
    /// its early returns cannot escape the clock above.
    fn on_feed_line(&mut self, line: &str, quoters: &mut [Quoter], by_market: &ByMarket) {
        // THE merge point: everything that reaches the engine passes
        // here exactly once, so this is where the WAL sequence is
        // assigned — before any parsing, so lines the engine skips are
        // still in the incident record verbatim.
        if let Some(w) = self.wal.as_mut() {
            w.append(line);
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else { return };
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
        // ...and the venue's answer to a halt SWEEP:
        //   {"kind":"sweep_result","venue":...,"ok":bool,"error":str|null,
        //    "ts_local_ns":int}
        // Read here, above the market guard, because a sweep is the
        // account-wide command and names no market. It is the only thing that
        // discharges a `sweeps_owed` entry.
        if kind == "sweep_result" {
            self.on_sweep_result(&v, venue);
            return;
        }
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
                self.on_order_ack(&v, ts_local_ns);
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
                self.on_cancel_result(&v);
                return;
            }
            // ...and the venue's answer to a PLACE it refused, but ONLY when
            // that refusal is one no retry can fix:
            //   {"kind":"place_result","venue":...,"market_id":str,
            //    "order_id":str,"ok":false,"retry":"market_halted",
            //    "error":str,"ts_local_ns":int}
            // A place normally answers with an `order_ack` or with nothing, and
            // "nothing" is indistinguishable from "not filled yet" — which is
            // how one halted Kalshi market drew 335 identical hedge places on
            // 2026-07-30. `Retry` in arb-venue is what makes the distinction.
            "place_result" => {
                self.on_place_result(&v);
                return;
            }
            "fill" => {
                self.on_fill(&v, venue, &market_id, ts_local_ns);
                return;
            }
            _ => return,
        }
        self.n_book += 1;
        // The re-mark trigger. A set lookup and a bool store — the rebuild and
        // the file write are `marks_tick`'s, on a deadline. UNGATED by `killed`
        // and `feed_reason`, unlike quoting and take-take below: a halted engine
        // still holds positions, and freezing their marks is how the take-take
        // bar goes stale while the process is alive to keep it fresh.
        if !self.marks_dirty
            && self.marks_watch.get(&venue).is_some_and(|s| s.contains(market_id.as_str()))
        {
            self.marks_dirty = true;
        }
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
                //
                // `rests_on: None` — leg 1 is a marketable IOC, so it does
                // not rest and reserves nothing. Nothing would ever release a
                // reservation for it: an IOC that does not fill dies at the
                // venue and produces no cancel, and reserving would COLLIDE
                // with the maker quote on the same leg, which shares the slot
                // key (see `MakerOrder::rested`).
                //
                // The window between this check and the fill that books it is
                // therefore still unreserved. `tt_gate` narrows it PER
                // RELATIONSHIP only — it stops the same crossing re-firing on
                // every book event, and does nothing about N relationships
                // each firing one clip against the same unmoved exposure.
                let v = rv.check(&quoters[qi].rel, c.lead, c.size, None);
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

    /// The kill-switch watch — and, on the same 1s period, the retry for any
    /// halt sweep no venue has proven. Both in-process halts end in the same
    /// owed sweep, and this is the deadline that is due while one is.
    ///
    /// ONLY WHILE THE HALT IS STILL IN FORCE, and that gate is the reason a
    /// venue failure could not simply be retried where a full channel is. A
    /// halt can clear with a sweep still owed, and on the venue-failure path it
    /// is the likely case rather than the unlucky one: the outage that made the
    /// venue refuse the sweep is the same outage whose recovery clears the pull.
    /// A sweep offered after that point is an account-wide cancel-all landing on
    /// quotes the quoter has just re-rested — and the engine would go on
    /// believing they rest, because a sweep tells it nothing about individual
    /// orders. That is the state `Engine::new` records markets going quietly
    /// dark from, and it is self-inflicted: the engine sees no reason to
    /// re-quote something it thinks is already working.
    ///
    /// The obligation is KEPT rather than dropped, because nothing has proven
    /// this book: `sweeps_owed` goes on saying so, and the next halt offers it
    /// again with a fresh backoff. What is NOT closed is the window inside one
    /// offer — a sweep queued while halted can still be dequeued after the halt
    /// clears — which is the pre-existing shape of every halt sweep and not
    /// something the engine can decide from here.
    ///
    /// The retry runs BELOW the kill-file read, so "still in force" means this
    /// tick and not the last one. Above it, the tick that finally notices an
    /// operator's removal still reads `self.killed == true` and would dispatch
    /// one last sweep on the way out — the exact offer the gate exists to
    /// refuse, one tick late.
    fn kill_tick(&mut self, quoters: &mut [Quoter]) {
        let kill_now = std::path::Path::new(&self.cfg.kill_file).exists();
        if kill_now && !self.killed {
            self.killed = true;
            eprintln!(
                "[engine] KILL switch on ({}) — cancelling all resting quotes",
                self.cfg.kill_file
            );
            // The per-order cancels below reach only orders we still
            // hold ids for, and NONE of them is verified. On 2026-07-28
            // that path logged "cancelled" for a PM-US order that was
            // still resting 35 minutes later, which is how the engine
            // can report itself halted while it is still exposed.
            //
            // So the real venue sweep goes FIRST — it proves the book
            // empty, and it reaches orders we hold no id for at all.
            // Halting is the one moment where "probably cancelled" is
            // not good enough, and the one moment where the sweep must
            // not be queued behind everything it makes redundant.
            self.owe_sweeps("KILL");
            for q in quoters.iter_mut() {
                q.cancel_all(&mut self.cx, self.last_now, &mut self.intents);
                self.drain_intents(Some(&q.rel));
            }
        } else if !kill_now && self.killed {
            self.killed = false;
            eprintln!("[engine] KILL switch cleared — quoting resumes");
        }
        if self.killed || self.feed_reason.is_some() {
            self.sweep_owed_venues();
        }
    }

    /// The stats line, and the take-take bar it re-derives.
    fn stats_tick(&mut self) {
        println!("{}", self.summary());
        // The tick window closes with the line that reports it. A
        // `tokio::time::interval`'s first tick is ready immediately, so every
        // timer arm does its most expensive work — a cold file read apiece —
        // inside `run`'s first budget; a process-lifetime max would name that
        // startup tick for the rest of the run and answer "what is slow now"
        // with "something was slow at 09:24". That is the defect this whole
        // ticket chased, and it is not worth reintroducing under a new name.
        // `decision`/`queue_wait` stay cumulative: their pinning is the
        // EVIDENCE (see `Engine::queue_wait`), not a bug to be windowed away.
        self.tick = Hist::new();
        self.slowest_tick = ("none", 0);
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
        for line in self.unwind_tick() {
            eprintln!("{line}");
        }
    }

    /// Rewrite `marks.json` from the live books, if a marked market has moved
    /// since the last write or the heartbeat is due.
    ///
    /// The DECISION about when is here; the arithmetic is `crate::marks`. It
    /// returns nothing and prints nothing on the happy path: this runs at up to
    /// 1 Hz, and a line per write would be 86,400 lines a day saying "still
    /// working". What it does emit is edge-triggered, and it is the failure —
    /// the standing signal is `marks_error` in the summary JSON.
    ///
    /// **NOT gated on `killed` / `feed_reason`, unlike `unwind_tick`.** Those
    /// gates exist because an exit is an order, and a halting engine stops
    /// sending orders. Marking sends nothing. A halted engine still HOLDS the
    /// positions, and it is the only process left that can keep their marks
    /// current — freezing them is precisely how a stale bar outlives the
    /// condition that caused it.
    ///
    /// **The staleness guard becomes self-referential when `out_path` is the
    /// file this engine also reads.** `taketake::MAX_MARKS_AGE_S` exists because
    /// `mark_positions.py` died on 2026-07-28 and the armed engine spent four
    /// hours trading against the frozen bar it left behind. Pointed at its own
    /// output, the engine can no longer be the thing that NOTICES a dead marker
    /// — but the guard is not thereby useless, and the distinction matters: it
    /// still catches this loop wedging while the process lives (the write stops,
    /// `generated_at` freezes, 900 s later take-take refuses), which is the case
    /// it can act on. What it cannot catch is the process dying, and a dead
    /// process is not trading either.
    fn marks_tick(&mut self) {
        let Some(m) = self.cfg.marks_out.clone() else { return };
        let now = wall_now();
        let since = now - self.marks_written_at;
        if since < m.min_interval_s {
            return;
        }
        if !self.marks_dirty && since < m.max_idle_s {
            return;
        }
        // Re-read the ledger only when it has actually changed. `marks_dirty`
        // tracks the BOOKS; a basket booked by this engine or appended by
        // another writer moves neither the books nor that bit, so this stat is
        // the only thing that notices it — and it is why the heartbeat exists
        // as well as the trigger.
        let stamp = std::fs::metadata(&m.ledger_path).ok().map(|md| {
            let mtime = md
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_nanos() as i64);
            (md.len(), mtime)
        });
        if stamp != self.marks_ledger_stamp {
            self.marks_ledger_stamp = stamp;
            // The LENIENT read (`ledger::read` is the strict one, and it gates
            // ARMING). A torn line here must not stop the whole book being
            // marked: the strict reader already refused to arm on it, and
            // withholding every other position's mark on top of that would take
            // the take-take bar down with it.
            self.marks_records = match std::fs::read_to_string(&m.ledger_path) {
                Ok(text) => {
                    text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
                }
                Err(_) => Vec::new(),
            };
            self.marks_watch = crate::marks::watched_markets(self.marks_records.clone());
        }
        // NOT YET, if this process has never seen a book. The first tick is due
        // immediately and `socket_feed`'s welcome snapshot burst takes about a
        // second to land, so without this a restart publishes one marks file in
        // which EVERY row is unpriced — throwing away the prices the previous
        // writer had put there — and shouts `NO BOOK` naming all 14 held
        // markets, when in truth it has simply not looked yet. Observed exactly
        // that on the 2026-07-31 live run.
        //
        // Below the ledger read on purpose: the watch set is what makes a book
        // event a trigger, so returning above it would mean no book event ever
        // arms a mark and this guard would never lift.
        //
        // `n_marks` and not a flag: once this engine has written once it is
        // committed to keeping the file current, halted or not (see above).
        if self.n_marks == 0 && self.n_book == 0 {
            return;
        }
        let marked = crate::marks::build(
            &mut self.cx,
            &self.fees,
            self.marks_records.clone(),
            &self.books,
            now,
        );
        self.marks_unpriced_rows =
            marked.doc.positions.iter().filter(|p| p.liq_value_usd.is_none()).count();
        // Edge-triggered on the SET, not the count: two markets going dark and
        // two different ones coming back is a change worth a line, and the count
        // alone cannot see it.
        if marked.no_book != self.marks_no_book {
            if marked.no_book.is_empty() {
                eprintln!("[marks] every held market is on the feed again");
            } else {
                // The old text here blamed the recorder's tag-driven PM-US
                // universe and pointed at mark_positions.py as the way out.
                // Both are wrong now: the cause was that the recorder's book set
                // was "markets that ticked since connect", fixed by the REST
                // seed in PR #60, and mark_positions.py is retired. Left
                // pointing at what an operator can actually check.
                eprintln!(
                    "[marks] NO BOOK for {} market(s) the open baskets hold — {} row(s) \
                     published UNPRICED: {}. Expected only in the seconds after a \
                     (re)connect, before the welcome burst lands; if it persists, the \
                     recorder is not carrying a market we hold — check its \
                     `[pmus-seed] seeded N book(s)` line and that the market is in \
                     config/registry.yaml.",
                    marked.no_book.len(),
                    self.marks_unpriced_rows,
                    marked.no_book.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
            self.marks_no_book = marked.no_book;
        }
        let err = crate::marks::write_atomic(&m.out_path, &marked.doc.to_json()).err();
        if err.is_some() != self.marks_error.is_some() {
            match &err {
                Some(why) => eprintln!("[marks] WRITE FAILED — {why}"),
                None => eprintln!("[marks] writing again"),
            }
        }
        self.marks_error = err;
        if self.marks_error.is_none() {
            self.n_marks += 1;
            self.marks_written_at = now;
        }
        self.marks_dirty = false;
    }

    /// The opportunistic-unwind scan: which open baskets have stopped being the
    /// best use of the capital they lock.
    ///
    /// DETECT ONLY. It moves gauges and RETURNS the lines the operator should
    /// see; it places nothing, and nothing it produces reaches an executor —
    /// see `crate::unwind` for why an exit is not an `Intent` yet, and for the
    /// five things that must be true before one could be.
    ///
    /// It returns the report rather than printing it because the report IS the
    /// deliverable of a detect-only feature, and a feature whose only output is
    /// `eprintln!` has nothing a test can assert on. The caller prints.
    ///
    /// It rides the stats tick because both of its inputs move on that
    /// timescale and neither moves on a book event: `arbbot-marks.timer`
    /// rewrites the forward APRs every two minutes, and the hurdle is
    /// `apr_tick`'s, which floats with utilization as baskets book. Putting it
    /// on the book-event path — where take-take lives, because a crossing is
    /// gone in milliseconds — would re-derive the same answer thousands of
    /// times a second from files that had not changed.
    fn unwind_tick(&mut self) -> Vec<String> {
        let Some(u) = self.cfg.unwind.as_ref() else { return Vec::new() };
        // NOT EXEMPT FROM THE HALT PATH. A killed or feed-pulled engine has
        // cancelled its book and is trying to stop; an exit is still an order,
        // so it stops too. Today that silences only a log line — the gate is
        // here because it belongs with the DECISION, and a placer added later
        // must not have to remember to add it.
        if self.killed || self.feed_reason.is_some() {
            self.clear_unwind_gauges();
            return Vec::new();
        }
        let marks = std::fs::read_to_string(&u.marks_path).unwrap_or_default();
        // The cap `utilization()` divides by, straight from the risk view — NOT
        // re-derived. `select` refuses when it is not a real number, because
        // `utilization()` answers a degenerate cap with 1.0 and that pins the
        // exit hurdle at its CEILING. No risk view at all (bench/replay) is the
        // same refusal for the same reason: there is no cap, so there is no
        // opportunity cost to charge.
        let cap = self.cfg.risk.as_ref().map_or(0.0, |r| r.global_cap_usd());
        // `self.apr_bar` and not a fresh `apr_bar(utilization())`: the exit is
        // measured against the hurdle a fresh quote is ACTUALLY being held to
        // right now, which is the one `apr_tick` installed on the quoters. Two
        // derivations of "the bar" could disagree about the NUMBER. They cannot
        // disagree about its safe DIRECTION, which is why the guard above is
        // not redundant with entry's — see `crate::unwind` §4.
        let sel = crate::unwind::select(&marks, self.apr_bar, cap, &u.owned_prefixes, wall_now());
        let refused = sel.as_ref().err().cloned();
        let mut out = Vec::new();
        // Edge-triggered, like the take-take bar: the reason carries an age
        // that moves every tick.
        if refused.is_some() != self.unwind_refused.is_some() {
            out.push(match &refused {
                Some(why) => format!("[unwind] NO SCAN — cannot decide: {why}"),
                None => "[unwind] inputs usable again — scanning".to_string(),
            });
        }
        self.unwind_refused = refused;
        let Ok((exits, skips)) = sel else {
            self.clear_unwind_gauges();
            return out;
        };
        let tally = crate::unwind::SkipTally::of(&skips);
        self.n_unwind = exits.len();
        self.n_unwind_ct = exits.iter().map(|e| e.qty).sum();
        self.n_unwind_actionable = exits.iter().filter(|e| e.actionable).count();
        self.n_unwind_near_floor = exits.iter().filter(|e| e.near_floor).count();
        self.n_unwind_near_miss = tally.exit_unprofitable;
        self.unwind_near_miss_usd = tally.exit_unprofitable_usd;
        // THE CANDIDATE SET **AND** THE SKIP BREAKDOWN. Keying on the candidate
        // set alone made this whole report unreachable on a book that selects
        // nothing — which is every book the live marks have produced — because
        // `identity_set(&[])` equals the empty state it starts in. The counts
        // are what make "the whole book went dark" a change; the set is
        // canonically ordered because `select`'s display order ties on both its
        // keys for identically-sized baskets and a reorder is not a change.
        let report: UnwindReport = (crate::unwind::identity_set(&exits), tally.counts());
        if self.unwind_seen.as_ref() == Some(&report) {
            return out;
        }
        self.unwind_seen = Some(report);
        // The skips go out with the candidates, not instead of them: "nothing
        // selected" has five causes and they are not the same finding.
        if exits.is_empty() {
            out.push(format!(
                "[unwind] nothing to exit at the {:.2}%/yr hurdle — {}",
                self.apr_bar,
                tally.describe()
            ));
            return out;
        }
        for e in &exits {
            let (market, side) = e.suppress_key();
            out.push(format!(
                "[unwind] WOULD EXIT {} x{} fwd={:.1}%/yr (hurdle {:.2}%/yr) \
                 exit={:+.4}/ct — rest {market}:{}{}{}{} [DETECT ONLY — nothing sent]",
                e.rel_id,
                e.qty,
                e.fwd_apr,
                self.apr_bar,
                e.exit_ct,
                side.as_str(),
                // The three things that make a candidate less than a decision.
                if e.actionable { "" } else { " [NOT OURS — outside --rel-prefix]" },
                if e.near_floor { " [NOISE — within one tick of the floor]" } else { "" },
                if e.resolves_estimated { " [APR rests on an ESTIMATED resolve date]" } else { "" },
            ));
        }
        out.push(format!("[unwind] skipped: {}", tally.describe()));
        out
    }

    /// Every unwind gauge back to zero, for the paths that decide nothing: the
    /// halt gates and a refusal. A gauge left at its last value would report a
    /// candidate set the engine has stopped standing behind.
    fn clear_unwind_gauges(&mut self) {
        self.n_unwind = 0;
        self.n_unwind_ct = 0;
        self.n_unwind_actionable = 0;
        self.n_unwind_near_floor = 0;
        self.n_unwind_near_miss = 0;
        self.unwind_near_miss_usd = 0.0;
        // `None` and not an empty report: when the halt clears, the next scan
        // must SPEAK rather than compare equal to whatever it happened to be
        // holding. That equality is exactly what silenced this feature.
        self.unwind_seen = None;
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
/// of it into the 65536-deep channel unpaced. Every armed start since
/// 2026-07-28T19:41 has reported a `chan_high_water` between 1710 and 2377 at
/// its FIRST stats line — a real queue, and without a budget it is that many
/// events before `data/KILL` is stat'ed, `health_tick` runs, or a hedge retry
/// or naked alarm can fire. What would stop the naked alarm is the market feed
/// misbehaving, which is the one condition it exists to survive (see
/// `engine::hedge`).
///
/// The multi-second `decision_latency` maxima are NOT that queue and must not
/// be quoted as evidence for this budget: they were never measured at dequeue.
/// See `Engine::queue_wait`, which now measures the two separately and says
/// what is still unexplained.
///
/// Sizing it, honestly. Every figure below is PRE-PR `decision_latency`, which
/// carried the queue wait inside it — so each is an UPPER bound on the decision
/// work this budget actually caps, which is the direction a safety bound may
/// err in, and post-PR the same percentile can only fall. Across the armed runs
/// since 2026-07-28T19:41, p50 is 24576ns (699 lines) or 49152ns (214) — 64
/// events is ~2-3ms. The p99 is where the earlier draft of this comment cheated:
/// it quoted 786us, which is the FLOOR of that distribution, not its shape. In
/// steady state (`elapsed_s >= 3600`, 622 lines) p99 runs 196608ns x189,
/// 393216 x286, 786432 x78, 3145728 x46 and 6291456 x23. At the WORST observed,
/// 64 x 6.29ms = ~400ms against the kill switch's 1s interval.
///
/// That still holds — but it holds by a factor of two, not "orders of magnitude
/// to spare". If this budget is ever raised, that is the number to raise it
/// against, and it should be re-derived from a post-PR dequeue-to-complete p99
/// rather than from this producer-stamped one. The cost stays one extra loop
/// iteration and six timer polls per 64 events — noise against a JSON parse per
/// event.
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
    // Well inside TOXGATE_MAX_AGE (120s), so a feed that is being written stays
    // installed and one that stopped is noticed long before the next quote.
    let mut tox_iv = tokio::time::interval(std::time::Duration::from_secs(30));
    tox_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The APR bar moves only when a basket books or the day rolls over, so a
    // minute is ample; what matters is that it moves at all.
    let mut apr_iv = tokio::time::interval(std::time::Duration::from_secs(60));
    apr_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Marking is BOOK-driven; this deadline only bounds how often the file is
    // rewritten (`marks_tick` owns both bounds and returns immediately when
    // neither is due). 500ms so the `min_interval_s` floor is met promptly
    // rather than up to a second late — a timer coarser than the floor would
    // make the real cadence the timer's, not the policy's.
    let mut marks_iv = tokio::time::interval(std::time::Duration::from_millis(500));
    marks_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let marking = eng.cfg.marks_out.is_some();

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
            // Each timer arm is timed and named. `decision_latency` describes
            // the feed arm alone, so before this a handler that held the loop
            // was visible only as everyone else's `queue_wait` — the exact
            // "blocked or merely idle-then-busy" ambiguity this loop could not
            // answer about itself.
            _ = kill_iv.tick() => { let t = std::time::Instant::now(); eng.kill_tick(&mut quoters); eng.record_tick("kill", t); }
            _ = hedge_iv.tick(), if hedge_retry && !bench => { let t = std::time::Instant::now(); eng.hedge_tick(); eng.record_tick("hedge", t); }
            // Fills held for an `order_ack` that has not come. Bench has no live
            // ack path at all and must stay byte-deterministic, so it relies on
            // the flush after the loop instead of this deadline.
            _ = fill_iv.tick(), if !bench => { let t = std::time::Instant::now(); eng.unclaimed_tick(); eng.record_tick("fill", t); }
            // Cancels the engine owes but could not address when it decided on
            // them. Only an armed engine can ever learn a venue id, so only an
            // armed engine parks (see `resolve_cancel`) and only it has anything
            // to do here. `cancel_work` owns the policy — including the one
            // escalation per tick and none at all while killed.
            _ = cancel_iv.tick(), if armed => { let t = std::time::Instant::now(); eng.cancel_tick(); eng.record_tick("cancel", t); }
            // Two independent facts, in order of locality: whether the engine's
            // own subscription can be trusted, then whether the recorder says
            // the venue sockets can be. Ungated by `--health` (only by bench)
            // because the FIRST of those is the engine's own business — a run
            // without a health file must still be able to notice, and clear, a
            // disconnect of its own feed.
            _ = feed_iv.tick(), if !bench => { let t = std::time::Instant::now(); eng.health_tick(&mut quoters); eng.record_tick("health", t); }
            // Off in bench/replay for the same reason as the two above: it
            // re-reads a file another process rewrites, which no pinned tape
            // can reproduce. Bench keeps whatever `install_policy` installed.
            _ = tox_iv.tick(), if !bench => { let t = std::time::Instant::now(); eng.tox_tick(&mut quoters); eng.record_tick("tox", t); }
            // Same rule: `cfg.apr` is already None in bench, and re-sizing a
            // hurdle mid-replay off a moving utilization would break the
            // digest even if it were not.
            _ = apr_iv.tick(), if !bench => { let t = std::time::Instant::now(); eng.apr_tick(&mut quoters); eng.maker_exit_tick(&mut quoters); eng.record_tick("apr", t); }
            // Same rule as the three above: it reads the wall clock and a
            // ledger another process appends to, and it writes a file — none of
            // which a byte-deterministic replay can reproduce. `marking` is
            // already false in bench (`run_cfg`); the guard is belt to that
            // brace, and it keeps the arm out of the poll set entirely.
            _ = marks_iv.tick(), if marking && !bench => { let t = std::time::Instant::now(); eng.marks_tick(); eng.record_tick("marks", t); }
            _ = stats_iv.tick(), if !bench => { let t = std::time::Instant::now(); eng.stats_tick(); eng.record_tick("stats", t); }
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
        toxgate_file: None,
        apr: None,
        apr_installed: (0.0, String::new()),
        risk: None,
        ledger_path: None,
        hedge_retry: None,
        take_take: None,
        suppress: Default::default(),
        maker_exit_view: false,
        unwind: None,
        marks_out: None,
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
            toxgate_file: None,
            apr: None,
            apr_installed: (0.0, String::new()),
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
            suppress: Default::default(),
            maker_exit_view: false,
            unwind: None,
            marks_out: None,
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

    /// Every command each venue's executor was sent, IN ORDER. `sweeps` above
    /// counts; this is for the tests that turn on which command went FIRST.
    fn commands(rxs: &mut [(Venue, mpsc::Receiver<ExecCmd>)]) -> Vec<(Venue, Vec<&'static str>)> {
        let mut out = Vec::new();
        for (v, rx) in rxs.iter_mut() {
            let mut cmds = Vec::new();
            while let Ok(c) = rx.try_recv() {
                cmds.push(match c.action {
                    Action::SweepAndVerify => "sweep",
                    Action::Cancel { .. } => "cancel",
                    Action::Place { .. } => "place",
                });
            }
            out.push((*v, cmds));
        }
        out.sort_by_key(|(v, _)| v.as_str());
        out
    }

    /// Executor channels with no room in them: one slot, already occupied, so
    /// every `try_send` fails until something drains it. This is the 1024-slot
    /// live channel under the backlog that `chan_high_water: 1036` measured.
    fn full_executors() -> Executors {
        let mut txs = HashMap::new();
        let mut rxs = Vec::new();
        for v in [Venue::Kalshi, Venue::PolymarketUs] {
            let (tx, rx) = mpsc::channel(1);
            tx.try_send(ExecCmd {
                t_read: std::time::Instant::now(),
                // Anything but a sweep, so the assertions below cannot read
                // the filler as the thing they are looking for.
                action: Action::Cancel {
                    req: arb_venue::gateway::CancelRequest {
                        by: arb_venue::gateway::CancelBy::ClientId("backlog".into()),
                        market_slug: None,
                    },
                    attempt: 0,
                },
            })
            .expect("the filler takes the only slot");
            txs.insert(v, tx);
            rxs.push((v, rx));
        }
        (txs, rxs)
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

    /// **THE ORDER OF A HALT.** The sweep goes out BEFORE the per-order
    /// cancels, and it used to go out last.
    ///
    /// Both land in one per-venue channel drained at `--rate` (8/s live) by a
    /// loop that awaits each venue call inline, so a sweep queued behind N
    /// cancels is dequeued N/8 seconds after the halt — and every one of those
    /// cancels is subsumed by the account-wide `cancel_all_open` the sweep
    /// issues on both venues. The whole rate budget was being spent on
    /// redundant work in front of the one operation that proves anything: the
    /// live armed engine emitted 21-23 cancels per pull on 2026-07-28
    /// (11-12 per venue, `data/trader-rs/m3-intents.jsonl`), which is ~1.4s of
    /// resting, fillable orders after the engine had declared itself pulled.
    #[tokio::test]
    async fn a_pull_queues_the_sweep_before_the_cancels_it_makes_redundant() {
        let out = scratch("sweep-first-intents.jsonl");
        let ts = 1_785_211_200.0;
        // A book on BOTH legs, so the quoter really rests quotes for the pull
        // to cancel — with nothing resting this test would pass on any code.
        let feed = [
            json!({"kind": crate::feed::FEED_UP}).to_string(),
            snapshot("kalshi", "K", "0.02", "0.03", ts),
            snapshot("polymarket_us", "P", "0.08", "0.09", ts),
            json!({"kind": crate::feed::FEED_DOWN, "note": "subscriber dropped"}).to_string(),
        ];
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        for l in &feed {
            tx.try_send(FeedMsg { line: l.clone(), t_read: std::time::Instant::now() })
                .expect("test channel");
        }
        drop(tx);
        let (txs, mut rxs) = executors();
        let summary = run(quoters, by_market, rx, txs, stats(), cfg(&out, None, None)).await;
        assert_eq!(summary["feed_pulled"], json!(true), "{summary}");
        for (venue, cmds) in commands(&mut rxs) {
            // The quote this pull is cancelling was PLACED through the same
            // channel and is still in front of both — that is ordinary
            // pre-halt work, not part of the halt.
            let cancel_at = cmds
                .iter()
                .position(|c| *c == "cancel")
                .unwrap_or_else(|| panic!("{venue:?} rested nothing, so this proves nothing: {cmds:?}"));
            let sweep_at = cmds
                .iter()
                .position(|c| *c == "sweep")
                .unwrap_or_else(|| panic!("{venue:?} was never swept at all: {cmds:?}"));
            assert!(
                sweep_at < cancel_at,
                "{venue:?}: the sweep must be queued AHEAD of the per-order cancels it \
                 makes redundant, not behind all of them: {cmds:?}"
            );
        }
    }

    /// **AN OBLIGATION THE ENGINE FORGOT IS NOT AN OBLIGATION DISCHARGED**, on
    /// the feed-stale pull — the hot path (five entries in twenty minutes of
    /// live arming on 2026-07-29).
    ///
    /// `try_send` LOSES the command when the executor's channel is full. The
    /// pull latched `feed_reason` anyway and the entry guard is an EDGE
    /// (`sweep = reason.is_some() && was.is_none()`), so nothing ever re-offered
    /// the sweep: the engine sat pulled, reporting itself pulled, over a venue
    /// book it had never swept — until the feed recovered AND broke again.
    #[tokio::test(start_paused = true)]
    async fn a_pull_sweep_the_executor_would_not_take_is_retried_until_it_lands() {
        let out = scratch("owed-pull-sweep-intents.jsonl");
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        for l in [
            json!({"kind": crate::feed::FEED_UP}).to_string(),
            snapshot("kalshi", "K", "0.03", "0.04", 1_785_211_200.0),
            json!({"kind": crate::feed::FEED_DOWN, "note": "subscriber dropped"}).to_string(),
        ] {
            tx.try_send(FeedMsg { line: l, t_read: std::time::Instant::now() })
                .expect("test channel");
        }
        let (txs, mut rxs) = full_executors();
        // The feed stays OPEN, so the deadline arms this turns on can fire.
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats(), cfg(&out, None, None)));

        // The pull happens with no room for its sweep: it is LOST.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 0), (Venue::PolymarketUs, 0)],
            "a full channel drops the sweep — that is the precondition, not the defect"
        );

        // ...and the executors have now drained (`sweeps` read the channels
        // empty), so a retry has somewhere to go.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)],
            "the pull is still owed a sweep and nothing else will ever offer it: the \
             entry guard is an edge the engine has already crossed"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["feed_pulled"], json!(true), "{summary}");
        assert_ne!(
            summary["exec_dropped"],
            json!(0),
            "...and a dropped sweep must MOVE the counter an operator watches. `dispatch` \
             holds the binary's only `dropped.fetch_add` and both halts used to send their \
             sweep around it, so `exec_dropped: 0` was never evidence that no sweep had \
             been dropped — the counter could not see the command: {summary}"
        );
    }

    /// The same rule on the KILL path, where `killed` latches on the way in and
    /// `kill_now && !self.killed` can never fire again: every stats line after
    /// this reads `"killed": true` while the quotes the engine could not prove
    /// gone are live and fillable.
    #[tokio::test(start_paused = true)]
    async fn a_kill_sweep_the_executor_would_not_take_is_retried_until_it_lands() {
        let kill = scratch("owed-KILL");
        std::fs::write(&kill, "halt").unwrap();
        let out = scratch("owed-kill-sweep-intents.jsonl");
        let mut cfg = cfg(&out, None, None);
        cfg.kill_file = kill.to_string_lossy().into_owned();

        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        let (txs, mut rxs) = full_executors();
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats(), cfg));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 0), (Venue::PolymarketUs, 0)],
            "the channel was full when the kill fired"
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)],
            "the kill is still owed a sweep: `killed` is latched, so nothing re-enters \
             this branch, and the book is never proven clean"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["killed"], json!(true), "{summary}");
    }

    /// The venue's answer to a sweep, built by the PRODUCER rather than
    /// re-spelled here: a hand-written copy of the schema would keep every one
    /// of these tests green through a field rename in `exec.rs`, which is the
    /// one change that would silently stop the engine ever discharging one.
    fn sweep_result(venue: Venue, err: Option<&str>) -> String {
        crate::exec::sweep_result(venue, err).to_string()
    }

    fn feed(tx: &mpsc::Sender<FeedMsg>, line: String) {
        tx.try_send(FeedMsg { line, t_read: std::time::Instant::now() }).expect("test channel");
    }

    /// Feed lines that take the engine from quoting to PULLED: connect, one
    /// book, then the disconnect.
    fn pull(tx: &mpsc::Sender<FeedMsg>) {
        for l in [
            json!({"kind": crate::feed::FEED_UP}).to_string(),
            snapshot("kalshi", "K", "0.03", "0.04", 1_785_211_200.0),
            json!({"kind": crate::feed::FEED_DOWN, "note": "subscriber dropped"}).to_string(),
        ] {
            feed(tx, l);
        }
    }

    /// **THE OTHER HALF: A SWEEP THE VENUE REFUSED IS NOT A BOOK PROVEN CLEAN.**
    ///
    /// The channel-full case above has been observed zero times. This one fired
    /// twice inside four minutes on 2026-07-29, on both venues, during a DNS
    /// outage: the sweep was QUEUED (the channel was empty, so the obligation was
    /// discharged on the spot), and 30s later the executor logged
    /// `KILL SWEEP FAILED — book could NOT be proven clean` for each. Neither
    /// this engine nor the retry above re-offered it, so `feed_pulled: true` sat
    /// over an unproven book for the rest of the outage.
    ///
    /// It also pins the SHAPE of the retry, which is not the same as the
    /// channel-full one: a refused `try_send` never touched a wire and is
    /// re-offered on the next kill tick for free, but a sweep the executor TOOK
    /// costs a real account-wide cancel-all plus a polled resting-list read on a
    /// shared, rate-limited account. At 1 Hz that four-minute outage would have
    /// bought 240 of them.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_the_venue_could_not_prove_is_owed_again_and_backs_off() {
        let out = scratch("failed-sweep-intents.jsonl");
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        pull(&tx);
        let (txs, mut rxs) = executors();
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats(), cfg(&out, None, None)));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)],
            "the pull offers one sweep per venue, and the channel has room for it"
        );

        // ...and the venue refuses it, which is what the 20s sweep budget
        // reports when a venue has stopped answering.
        for v in [Venue::Kalshi, Venue::PolymarketUs] {
            feed(&tx, sweep_result(v, Some("book could NOT be proven clean")));
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 0), (Venue::PolymarketUs, 0)],
            "a sweep that reached a venue and failed must NOT be re-offered at the 1 Hz \
             the kill watch runs at: it is a real API round trip on a shared account, and \
             a venue down for four minutes would buy 240 of them"
        );
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)],
            "...and it must be offered AGAIN once the backoff expires. Queueing a sweep \
             is not proving a book: the executor is the only thing that knows whether the \
             venue answered, and until it says so the halt still owes one"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["feed_pulled"], json!(true), "{summary}");
        assert_eq!(
            summary["sweeps_owed"],
            json!(2),
            "...and a halt that never proved its book must SAY so. `exec_dropped` cannot: \
             it moves only on a refused `try_send`, and this sweep was accepted. One \
             `eprintln!` was the entire trace: {summary}"
        );
    }

    /// **A SWEEP THAT HAS NOT ANSWERED IS STILL RUNNING, AND NOTHING BOUNDS HOW
    /// LONG.**
    ///
    /// The backoff cannot double as the in-flight interlock, because there is no
    /// duration to outlast. `SweepPolicy::budget` reads like a 20s cap and is
    /// not one: its guard is `rounds_done > 0 && ...`, so round 1 is never
    /// budget-checked, and Kalshi's `cancel_all_open` is 1+N HTTP requests that
    /// consult no clock. At the 15s transport timeout a slow-but-answering venue
    /// with ~10 resting orders spends minutes inside it. The sweep that motivated
    /// this PR took THIRTY seconds — the floor exactly, not comfortably inside it.
    ///
    /// A purely timed retry offers again while sweep #1 is still running and
    /// every offer SUCCEEDS: the channel has 1024 slots. The executor then drains
    /// them serially once the venue recovers — N account-wide cancel-alls landing
    /// after the halt cleared, on quotes the quoter has just re-rested and still
    /// believes in. Once per attempt, where base manages it only once per halt.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_that_has_not_answered_is_never_offered_again() {
        let out = scratch("inflight-sweep-intents.jsonl");
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        pull(&tx);
        let (txs, mut rxs) = executors();
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats(), cfg(&out, None, None)));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(sweeps(&mut rxs), vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)]);

        // The venue says NOTHING for five minutes — it is up, slow, and still
        // inside round 1. Every backoff in the design has expired several times.
        tokio::time::sleep(std::time::Duration::from_secs(300)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 0), (Venue::PolymarketUs, 0)],
            "not one more sweep may be queued while the last has not answered: they do \
             not overtake each other, they QUEUE, and the executor runs the backlog \
             serially into a halt that may since have cleared"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["sweeps_owed"], json!(2), "and it is still owed, not forgotten");
    }

    /// A sweep the venue PROVED discharges the obligation — the engine must not
    /// go on re-offering a sweep against a book that is already clean.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_the_venue_proved_clean_is_discharged() {
        let out = scratch("proven-sweep-intents.jsonl");
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        pull(&tx);
        let (txs, mut rxs) = executors();
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats(), cfg(&out, None, None)));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(sweeps(&mut rxs), vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)]);
        for v in [Venue::Kalshi, Venue::PolymarketUs] {
            feed(&tx, sweep_result(v, None));
        }
        tokio::time::sleep(std::time::Duration::from_secs(120)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 0), (Venue::PolymarketUs, 0)],
            "the book is proven empty; nothing is owed and nothing may be re-offered"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["sweeps_owed"], json!(0), "{summary}");
    }

    /// **THE HALT CAN CLEAR WHILE A SWEEP IS STILL OWED**, and on this path it is
    /// LIKELY: the outage that made the venue refuse the sweep is the same outage
    /// whose recovery clears the pull.
    ///
    /// A sweep offered after that point is an account-wide cancel-all against a
    /// book the quoter has just re-quoted — and the engine would go on believing
    /// those orders rest, because nothing tells it otherwise. That is the state
    /// `Engine::new` documents markets going quietly dark from. So the retry is
    /// gated on the halt still being in force, and the obligation is KEPT rather
    /// than dropped: the book was never proven, the gauge says so, and the next
    /// halt offers it again.
    ///
    /// Driven off the KILL file rather than the feed pull, because that is the
    /// halt whose clearing a test can actually reach: `resync_reason` measures
    /// the welcome burst with `std::time::Instant`, which `start_paused` does
    /// not move, so a feed pull can never settle inside a paused test.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_still_owed_when_the_halt_clears_is_kept_but_not_fired() {
        let kill = scratch("cleared-halt-KILL");
        std::fs::write(&kill, "halt").unwrap();
        let out = scratch("cleared-halt-sweep-intents.jsonl");
        let mut cfg = cfg(&out, None, None);
        cfg.kill_file = kill.to_string_lossy().into_owned();
        // The one mode whose feed health cannot pull on its own: a live engine
        // starts `Link::Down` and pulls before the first tick, which would
        // leave a second halt in force and nothing to observe.
        cfg.bench = true;

        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        let (txs, mut rxs) = executors();
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats(), cfg));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(sweeps(&mut rxs), vec![(Venue::Kalshi, 1), (Venue::PolymarketUs, 1)]);
        for v in [Venue::Kalshi, Venue::PolymarketUs] {
            feed(&tx, sweep_result(v, Some("book could NOT be proven clean")));
        }
        // ...and the operator lifts the halt while the book is still unproven.
        std::fs::remove_file(&kill).unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(120)).await;
        assert_eq!(
            sweeps(&mut rxs),
            vec![(Venue::Kalshi, 0), (Venue::PolymarketUs, 0)],
            "the halt has cleared and the quoter may rest fresh orders: a sweep now would \
             cancel them at the venue while the engine went on believing they rest"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["killed"], json!(false), "the halt cleared: {summary}");
        assert_eq!(
            summary["sweeps_owed"],
            json!(2),
            "...and the obligation is KEPT, not forgotten: this book was never proven \
             clean by anything, and the next halt is what offers it again: {summary}"
        );
    }

    /// The halt gate must read THIS tick's halt, not the last one's.
    ///
    /// `kill_tick` used to retry before re-reading the kill file, so on the tick
    /// that finally notices an operator's removal `self.killed` was still true
    /// and one last sweep went out — the precise offer the gate exists to
    /// refuse, one tick late, and landing on a book the engine is about to start
    /// re-quoting.
    ///
    /// Driven on the channel-full path because that is the one always DUE: a
    /// refused offer keeps `wait_ticks` at 0, so the stale tick is reachable
    /// rather than a coincidence of the backoff. Refusals are counted by
    /// `exec_dropped`, and the file is removed with no await in between, so
    /// `before` is exactly the state the noticing tick starts from.
    #[tokio::test(start_paused = true)]
    async fn the_halt_gate_is_read_on_the_tick_it_fires_not_the_one_before() {
        let kill = scratch("stale-gate-KILL");
        std::fs::write(&kill, "halt").unwrap();
        let out = scratch("stale-gate-intents.jsonl");
        let mut cfg = cfg(&out, None, None);
        cfg.kill_file = kill.to_string_lossy().into_owned();
        cfg.bench = true; // no feed pull, so `killed` is the only halt in force

        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        let (txs, _rxs) = full_executors(); // never drained: every offer is refused
        let stats = stats();
        let engine = tokio::spawn(run(quoters, by_market, rx, txs, stats.clone(), cfg));

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let before = stats.dropped.load(std::sync::atomic::Ordering::Relaxed);
        assert!(before > 0, "the halt is in force and its sweep is being refused every tick");
        // No await between the read and the removal: the engine task cannot run
        // in the gap, so the next tick is the one that notices.
        std::fs::remove_file(&kill).unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert_eq!(
            stats.dropped.load(std::sync::atomic::Ordering::Relaxed),
            before,
            "the tick that notices the halt has cleared must not dispatch on its way out"
        );

        drop(tx);
        let summary = engine.await.expect("engine task");
        assert_eq!(summary["killed"], json!(false), "{summary}");
    }

    /// A CLOSED executor channel is not a full one. `try_send` returns false for
    /// both and the engine could not tell them apart, so a dead executor — the
    /// one state where no sweep is possible at all — retried at 1 Hz for ever,
    /// logging a line a second and moving `exec_dropped` with it.
    ///
    /// Close to unreachable (`install_armed_panic_hook` takes the process down
    /// first), but it is a mode the old fire-and-forget did not have.
    #[tokio::test(start_paused = true)]
    async fn a_closed_executor_is_not_retried_at_1hz() {
        let out = scratch("closed-exec-sweep-intents.jsonl");
        let (quoters, by_market) = fixture();
        let (tx, rx) = mpsc::channel(256);
        pull(&tx);
        let (txs, rxs) = executors();
        drop(rxs); // the executor tasks are gone; nothing can ever drain these
        let stats = stats();
        let engine =
            tokio::spawn(run(quoters, by_market, rx, txs, stats.clone(), cfg(&out, None, None)));

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let dropped = stats.dropped.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            dropped <= 4,
            "a sweep into a channel nobody will ever drain must back off like any other \
             offer that cannot succeed within a tick — 1 Hz for ten seconds is {dropped} \
             refusals and no sweep"
        );

        drop(tx);
        let _ = engine.await.expect("engine task");
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
    /// Live, this is the armed engine's own start: every armed run since
    /// 2026-07-28T19:41 reports a `chan_high_water` between 1710 and 2377 at
    /// its FIRST stats line, and under `biased` alone that is that many events
    /// before `data/KILL` — documented as a 1-second watch — is first stat'ed.
    /// (The multi-second `decision_latency` maxima are NOT this queue and were
    /// never measured at dequeue; see `Engine::queue_wait`.)
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

    /// **The ~3s `decision_latency` max was never a slow decision.**
    ///
    /// `t_read` is stamped by the PRODUCER in `feed`'s `tx.send(..).await`, so
    /// every sample carried the message's channel wait inside it. Live,
    /// `spawn_feed` (main.rs:1481) begins stamping the recorder's connect burst
    /// while `engine::run` (main.rs:1551) is still behind `arm_venues` and
    /// `startup_sweep().await` — so the first couple of thousand messages were
    /// charged the whole of armed startup and the `max` pinned there for the
    /// life of the process. Every armed run since 2026-07-28T19:41 shows it.
    /// One of them: at `elapsed_s: 0.0`, after 2185 `events` (2184 of them
    /// `book_events`), `max_ns` was already 3_004_381_840 — and at
    /// `elapsed_s: 4620.0` and 1_014_270 `events` it was still exactly
    /// 3_004_381_840. The two counters are named separately on purpose here:
    /// `decision_latency.count` tracks `events`, not `book_events`.
    ///
    /// A backdated stamp is exactly that shape. The wait must land in
    /// `queue_wait` and NOT in `decision_latency`, or the two causes the ticket
    /// could not separate — a blocked handler and a quiet feed behind a
    /// backlog — keep producing the identical number.
    #[test]
    fn a_message_that_waited_in_the_channel_is_not_a_slow_decision() {
        let out = scratch("queue-wait-intents.jsonl");
        let (mut quoters, by_market) = fixture();
        let mut eng = test_engine(cfg(&out, None, None));
        eng.on_feed(
            FeedMsg {
                line: snapshot("kalshi", "K", "0.03", "0.04", 1_785_211_200.0),
                t_read: std::time::Instant::now() - std::time::Duration::from_secs(3),
            },
            0,
            &mut quoters,
            &by_market,
        );
        let s = eng.summary();
        assert!(
            s["queue_wait"]["max_ns"].as_u64().unwrap() >= 3_000_000_000,
            "the 3s wait belongs to the channel, and must be reported: {s}"
        );
        assert!(
            s["decision_latency"]["max_ns"].as_u64().unwrap() < 1_000_000_000,
            "the decision itself parsed one snapshot; charging it the channel \
             wait is the defect this whole ticket chased: {s}"
        );
    }

    /// A histogram says A tick took 6s. Only the name says WHICH, and the name
    /// has to follow the maximum rather than the most recent sample.
    #[test]
    fn the_slowest_tick_names_the_arm_that_set_it() {
        let out = scratch("slowest-tick-intents.jsonl");
        let mut eng = test_engine(cfg(&out, None, None));
        let slow = std::time::Instant::now() - std::time::Duration::from_secs(2);
        eng.record_tick("tox", slow);
        // Later, and faster: the name must not follow it.
        eng.record_tick("kill", std::time::Instant::now());
        let s = eng.summary();
        assert_eq!(s["slowest_tick_window"]["arm"], json!("tox"), "{s}");
        assert!(s["slowest_tick_window"]["ns"].as_u64().unwrap() >= 2_000_000_000, "{s}");
        assert_eq!(s["tick_latency_window"]["count"], json!(2), "both arms timed: {s}");
    }

    /// ...and the arms in `run`'s select are actually wired to it. The
    /// bookkeeping test above passes just as well against a `record_tick` no
    /// select arm ever calls, which is the state this ticket found the loop in.
    #[tokio::test]
    async fn a_timer_arm_is_timed_at_all() {
        const BACKLOG: usize = 128 * FEED_BUDGET;
        let out = scratch("tick-wired-intents.jsonl");
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
        let summary = run(quoters, by_market, rx, txs, stats(), cfg(&out, None, None)).await;
        assert_ne!(
            summary["slowest_tick_window"]["arm"],
            json!("none"),
            "the backlog spends the budget, so a timer arm must have run AND \
             been timed: {summary}"
        );
        assert!(summary["tick_latency_window"]["count"].as_u64().unwrap() > 0, "{summary}");
    }

    /// **The tick metrics must not reintroduce the defect above.**
    ///
    /// A `tokio::time::interval`'s first tick is ready immediately, so
    /// `tox_tick`, `apr_tick` and `health_tick` all do their most expensive
    /// first work — a file read apiece, cold — inside `run`'s first budget. A
    /// process-lifetime running max would therefore pin to a startup tick and
    /// name it forever, which is EXACTLY the reading `decision_latency` gave
    /// for the whole of this ticket. Shipping that again under a new name is
    /// worse than not shipping the metric.
    ///
    /// So the window is one stats interval: after a line is printed, the next
    /// line describes the ticks since it. An operator asking "what is slow
    /// NOW" gets an answer about now.
    #[test]
    fn the_slowest_tick_is_a_window_not_a_lifetime_max() {
        let out = scratch("tick-window-intents.jsonl");
        let mut eng = test_engine(cfg(&out, None, None));
        // Startup: the first `tox_tick` reads its file cold.
        eng.record_tick("tox", std::time::Instant::now() - std::time::Duration::from_secs(3));
        eng.stats_tick();
        // ...and hours later the loop is healthy. The line must say so.
        eng.record_tick("kill", std::time::Instant::now());
        let s = eng.summary();
        assert_eq!(
            s["slowest_tick_window"]["arm"],
            json!("kill"),
            "a startup tick must not still be named an interval later: {s}"
        );
        assert!(
            s["slowest_tick_window"]["ns"].as_u64().unwrap() < 3_000_000_000,
            "and its time must not survive the window either: {s}"
        );
        assert_eq!(
            s["tick_latency_window"]["count"],
            json!(1),
            "the histogram is the same window as the name it sits next to: {s}"
        );
    }

    /// **Every line off the channel is measured, including the ones the engine
    /// skips.**
    ///
    /// `on_feed_line` has thirteen early returns: a control line, an
    /// unparseable line, a line with no venue, a line with no market, a delta
    /// whose book is not synced, and so on. The control line does the most
    /// work of any of them: `FEED_DOWN` pulls every quote and enqueues a cancel
    /// per resting order — 23 of them on the 2026-07-28T20:13:06 disconnect —
    /// so leaving that path out of `decision_latency` would leave the busiest
    /// of them invisible, in the one metric whose stated job is to make a stall
    /// attributable. (Busiest, not slowest: those 23 are `try_send`s. The ~1.0s
    /// that disconnect took to clear was the executor's rate-limited drain, not
    /// this handler — see `on_feed` and `owe_sweeps`.)
    ///
    /// The invariant is the assert: `decision_latency.count == events`. It
    /// costs nothing to hold and it is the difference between "the loop was
    /// fast" and "the loop was fast on the paths I remembered to time".
    #[test]
    fn every_line_off_the_channel_is_timed_including_the_skipped_ones() {
        let out = scratch("total-decision-intents.jsonl");
        let (mut quoters, by_market) = fixture();
        let mut eng = test_engine(cfg(&out, None, None));
        let lines = [
            json!({"kind": crate::feed::FEED_DOWN, "note": "test", "ts": 0.0}).to_string(),
            "{not json".to_string(),
            json!({"kind": "snapshot", "market_id": "K"}).to_string(),
            snapshot("kalshi", "K", "0.03", "0.04", 1_785_211_200.0),
        ];
        for line in lines {
            eng.on_feed(
                FeedMsg { line, t_read: std::time::Instant::now() },
                0,
                &mut quoters,
                &by_market,
            );
        }
        let s = eng.summary();
        assert_eq!(s["events"], json!(4), "{s}");
        assert_eq!(
            s["decision_latency"]["count"],
            s["events"],
            "a line the engine skipped still spent the loop's time: {s}"
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

/// **A gate nothing reloads is a two-minute gate.**
///
/// `Toxgate` carries ONE `ts`, captured when the file was parsed, and
/// `Toxgate::verdict` will not score anything past `TOXGATE_MAX_AGE` (120s).
/// So even the wiring this PR adds would have gated for two minutes after
/// startup and then gone quiet forever. The file on disk when this was written
/// was stamped 2026-07-26 — three days behind — for exactly that reason:
/// nothing ever went back for it.
#[cfg(test)]
mod toxgate_reload_tests {
    use super::*;
    use arb_core::scan::{RelLeg, RelType};

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join(format!("arb-trader-toxreload-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A feed stamped `age` seconds ago, scoring the PM-US leg's bid at `score`.
    fn write_feed(path: &std::path::Path, age: f64, score: f64) {
        std::fs::write(
            path,
            format!(
                r#"{{"ts": {}, "markets": {{"P": {{"bid": {score}}}}}}}"#,
                wall_now() - age
            ),
        )
        .unwrap();
    }

    fn engine_watching(path: &std::path::Path) -> Engine {
        let mut cfg = test_cfg();
        cfg.toxgate_file = Some(path.to_string_lossy().into_owned());
        test_engine(cfg)
    }

    /// Same fixture the quoter's own tests use: Kalshi's 0.60 bid funds a
    /// PM-US maker YES bid one tick inside PM-US's 0.30.
    pub(super) fn quoter_and_books() -> (Vec<Quoter>, BookBuilder) {
        let rel = Rel {
            id: "xvus-france-pres-27-test".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        };
        let lvl = |p: &str, s: &str| Level { price: p.into(), size: s.into() };
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.60", "500")],
                          vec![lvl("0.99", "1")], 1, 1_000_000_000, None);
        bb.apply_snapshot(Venue::PolymarketUs, "P", vec![lvl("0.30", "500")],
                          vec![lvl("0.99", "1")], 1, 1_000_000_000, None);
        (vec![Quoter::new(rel)], bb)
    }

    /// Every way the feed can fail to be readable blocks the maker, and a
    /// reload — not a restart — is what clears it and what puts it back.
    #[test]
    fn a_feed_the_engine_cannot_read_blocks_the_maker_until_a_reload_clears_it() {
        let d = scratch("cycle");
        let p = d.join("toxgate.json");
        let mut quoters: Vec<Quoter> = Vec::new();

        // 1. No file at all. The engine starts PULLED, exactly as it does when
        //    the recorder health feed has not yet been proven.
        let mut eng = engine_watching(&p);
        assert!(eng.tox_reason.is_some(), "a missing feed must not read as permission");

        // 2. The three-day-old file that was actually on disk. No better.
        write_feed(&p, 3.0 * 86_400.0, 0.001);
        eng.tox_tick(&mut quoters);
        let why = eng.tox_reason.clone().expect("a 3-day-old feed must not permit");
        assert!(why.contains("old"), "and it must say the age refused it: {why}");

        // 3. The research writer comes back. THIS is the transition the frozen
        //    `ts` made unreachable: without a reload the engine would still be
        //    holding the document from step 2.
        write_feed(&p, 1.0, 0.001);
        eng.tox_tick(&mut quoters);
        assert_eq!(eng.tox_reason, None, "a current feed must clear the block");

        // 4. ...and it goes dark again. The gate has to notice BOTH ways, or
        //    it is a startup check wearing a gate's name.
        write_feed(&p, arb_core::quoter::TOXGATE_MAX_AGE + 1.0, 0.001);
        eng.tox_tick(&mut quoters);
        assert!(eng.tox_reason.is_some(), "a feed that stopped must block again");
    }

    /// The reload REPLACES what the quoter gates on, rather than only clearing
    /// a flag: a side that becomes toxic while the process runs stops being
    /// quoted without a restart.
    #[test]
    fn a_reload_replaces_the_scores_the_quoter_gates_on() {
        let d = scratch("scores");
        let p = d.join("toxgate.json");
        let (mut quoters, bb) = quoter_and_books();
        let mut eng = engine_watching(&p);

        let decide = |quoters: &mut Vec<Quoter>| {
            let mut cx = Cx::default();
            let fees = FeeSchedule::new(&mut cx);
            let (mut oid, mut intents) = (0u64, Vec::new());
            quoters[0].on_book(&mut cx, &fees, &bb, wall_now(), &mut oid, &mut intents);
            intents
        };

        write_feed(&p, 1.0, 0.001); // well under TOXGATE_MAX
        eng.tox_tick(&mut quoters);
        assert_eq!(eng.tox_reason, None);
        let clean = decide(&mut quoters);
        assert!(
            clean.iter().any(|i| matches!(i, Intent::Place(_))),
            "a clean side must quote: {clean:?}"
        );

        // The research feed rescores the same side at 7x the max — the range it
        // was actually reporting (0.0822-0.2199) while the gate was unwired.
        write_feed(&p, 1.0, 0.2199);
        eng.tox_tick(&mut quoters);
        let toxic = decide(&mut quoters);
        assert!(
            !toxic.iter().any(|i| matches!(i, Intent::Place(_))),
            "the reloaded score must gate: {toxic:?}"
        );
        assert!(
            toxic.iter().any(|i| matches!(i, Intent::Skip(s) if s.skip[0].contains("toxgate bid"))),
            "and say so: {toxic:?}"
        );
    }

    /// No `--toxgate` at all is the gate being OFF, which is not the same
    /// state as a gate that is on and cannot see. The golden and intent
    /// replays run this way and must keep deciding as they always have.
    #[test]
    fn the_gate_is_off_entirely_when_no_file_is_configured() {
        let mut eng = test_engine(test_cfg()); // toxgate_file: None
        assert_eq!(eng.tox_reason, None, "no gate configured is not a blocked gate");
        eng.tox_tick(&mut Vec::new());
        assert_eq!(eng.tox_reason, None, "and the tick is a no-op");
    }

    /// **A stale feed is REPORTED without stopping the book.**
    ///
    /// The first cut of this pulled every quote in the engine whenever the feed
    /// went unreadable. That is disproportionate by a factor this model cannot
    /// justify: it scores 35 of the 80 legs in the live registry, all Kalshi,
    /// and 38 of the rest are Polymarket / PM-US legs it has never scored and
    /// never will. So the reason is a GAUGE and a log line; the refusal is per
    /// (market, side) inside the quoter, where it can tell the two apart.
    ///
    /// The feed here is three days stale — the real file's age — and scores a
    /// Kalshi ticker this relationship does not touch.
    #[test]
    fn a_stale_feed_is_reported_without_stopping_the_book_it_never_covered() {
        let d = scratch("proportion");
        let p = d.join("toxgate.json");
        std::fs::write(
            &p,
            format!(
                r#"{{"ts": {}, "markets": {{"KXALIENS-27": {{"bid": 0.0584}}}}}}"#,
                wall_now() - 3.0 * 86_400.0
            ),
        )
        .unwrap();

        let (mut quoters, _) = quoter_and_books();
        let by_market: ByMarket = HashMap::from([
            ((Venue::Kalshi, "K".to_string()), vec![0usize]),
            ((Venue::PolymarketUs, "P".to_string()), vec![0usize]),
        ]);
        let feed = |venue: &str, market: &str, bid: &str| FeedMsg {
            line: serde_json::json!({
                "kind": "snapshot", "venue": venue, "market_id": market,
                "bids": [{"price": bid, "size": "500"}],
                "asks": [{"price": "0.99", "size": "1"}],
                "seq": 1, "ts_local_ns": (wall_now() * 1e9) as i64})
            .to_string(),
            t_read: std::time::Instant::now(),
        };

        let mut eng = engine_watching(&p);
        assert!(eng.tox_reason.is_some(), "the staleness must be visible");
        assert_eq!(
            eng.summary()["toxgate_stale"],
            serde_json::json!(true),
            "and standing, not just a startup line"
        );

        eng.on_feed(feed("kalshi", "K", "0.60"), 0, &mut quoters, &by_market);
        eng.on_feed(feed("polymarket_us", "P", "0.30"), 0, &mut quoters, &by_market);
        assert!(
            eng.n_int > 0,
            "a model that never scored this side must not silence it"
        );
    }
}

/// **A hurdle computed once at startup decays toward permissive.**
///
/// Both of its terms drift the same way: utilization RISES as baskets book,
/// and the hold SHORTENS every day. Python re-derived it on a timer
/// (`exec/main.py:_tt_refresh`, "makers clear the same bar"). Freezing it here
/// would repeat, in the same PR, the frozen-`ts` defect this change fixes for
/// the toxgate.
#[cfg(test)]
mod apr_refresh_tests {
    use super::toxgate_reload_tests::quoter_and_books;
    use super::*;

    /// A risk view on a REAL cap file ($980 bankroll x 0.35 = $343 class
    /// budget), with nothing at work yet.
    ///
    /// The file has to exist. This fixture originally passed
    /// `/nonexistent/exec.yaml` and leaned on the compiled-in defaults, which
    /// #19 (`f0f5593`) removed: an absent cap file is now `Caps::corrupt`, so
    /// bankroll and per_class are "0", `utilization()` reads that as a FULL
    /// book by design, and the bar comes out at the ceiling instead of the
    /// floor. That composition is correct and is pinned below; what it is not
    /// is an idle book, which is what this fixture is for.
    fn idle_risk() -> Arc<crate::risk::RiskView> {
        let d = std::env::temp_dir().join(format!("arb-trader-apr-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let exec = d.join("exec.yaml");
        std::fs::write(&exec, "bankroll_usd: 980\nper_class_cap: 0.35\n").unwrap();
        Arc::new(crate::risk::RiskView::load(
            &exec.to_string_lossy(),
            "/nonexistent/topics.yaml",
            Vec::new(),
            HashMap::new(),
        ))
    }

    fn engine_with(risk: Arc<crate::risk::RiskView>, quoters: &mut [Quoter]) -> Engine {
        let mut cfg = test_cfg();
        let (bar, asof, _) = crate::apply_apr(quoters, None, Some("2026-07-29"), Some(&risk));
        cfg.apr = Some(AprCfg { min_apr: None, asof: Some("2026-07-29".into()) });
        cfg.apr_installed = (bar, asof);
        cfg.risk = Some(risk);
        test_engine(cfg)
    }

    /// The bar an idle book asks for is the floor; the bar a full one asks for
    /// is higher, and the REFRESH is what moves it. Without `apr_tick` the
    /// engine would still be charging the startup bar after the book filled.
    #[test]
    fn a_book_that_fills_raises_the_bar_without_a_restart() {
        let (mut quoters, _) = quoter_and_books();
        let risk = idle_risk();
        let mut eng = engine_with(risk.clone(), &mut quoters);
        assert!(
            (eng.apr_bar - crate::APR_FLOOR).abs() < 1e-9,
            "an idle book starts at the floor, got {}",
            eng.apr_bar
        );

        // Half the $490 the engine may deploy ($980 x GLOBAL_CAP 0.50) goes to
        // work. NOT half the $343 class budget: `utilization()` sums every
        // class, so it divides by the cap that bounds every class.
        risk.record_open("xvus-france-pres-27-test", "cross-venue-equivalent", 245.0);
        eng.apr_tick(&mut quoters);
        let want = crate::apr_bar(0.5);
        assert!(
            (eng.apr_bar - want).abs() < 0.05,
            "the refresh must follow utilization: {} vs {want}",
            eng.apr_bar
        );
        assert!(eng.apr_bar > crate::APR_FLOOR, "and it must have MOVED");

        // ...and it is reportable, which it was not before: nothing this
        // process emitted said what hurdle was in force.
        assert!(
            (eng.summary()["maker_apr_bar"].as_f64().unwrap() - eng.apr_bar).abs() < 1e-9
        );
    }

    /// **A cap file this engine cannot read asks the MOST of a new quote.**
    ///
    /// `Caps::corrupt` (#19) forces bankroll and per_class to "0" so every cap
    /// refuses; `utilization()` reads a zero budget as a FULL book, so the
    /// hurdle goes to the ceiling rather than the floor. Both halves fail
    /// closed, and this pins that they fail closed in the SAME direction —
    /// a damaged `exec.yaml` must not hand the maker its cheapest bar.
    #[test]
    fn an_unreadable_cap_file_asks_the_ceiling_not_the_floor() {
        let risk = Arc::new(crate::risk::RiskView::load(
            "/nonexistent/exec.yaml",
            "/nonexistent/topics.yaml",
            Vec::new(),
            HashMap::new(),
        ));
        assert_eq!(risk.utilization(), 1.0, "a $0 budget is not an empty one");
        let (mut quoters, _) = quoter_and_books();
        let (bar, _, _) = crate::apply_apr(&mut quoters, None, Some("2026-07-29"), Some(&risk));
        assert!((bar - crate::APR_CEIL).abs() < 1e-9, "expected the ceiling, got {bar}");
    }

    /// No `cfg.apr` is bench/replay, where a moving bar would break the digest.
    #[test]
    fn a_run_with_no_apr_config_never_refreshes() {
        let (mut quoters, _) = quoter_and_books();
        let mut eng = test_engine(test_cfg()); // apr: None
        eng.apr_tick(&mut quoters);
        assert_eq!(eng.apr_bar, 0.0, "bench must not acquire a hurdle mid-replay");
    }
}

/// **The maker-exit seam.** What the engine publishes for `crate::maker_exit`,
/// and what it installs on the quoters when that module asks for a side.
#[cfg(test)]
mod maker_exit_seam_tests {
    use super::*;
    use crate::engine::toxgate_reload_tests::quoter_and_books;

    fn armed_cfg() -> RunCfg {
        RunCfg { maker_exit_view: true, bench: false, ..test_cfg() }
    }

    /// `RunCfg` is not `Clone` (it carries an `Arc<RiskView>` and a file path
    /// set the engine owns), and only the fields this test varies need copying.
    trait CloneForTest {
        fn clone_for_test(&self) -> RunCfg;
    }
    impl CloneForTest for RunCfg {
        fn clone_for_test(&self) -> RunCfg {
            RunCfg { suppress: self.suppress.clone(), ..armed_cfg() }
        }
    }

    /// A book on which the entry quoter genuinely WANTS the Kalshi ask.
    ///
    /// `quoter_and_books`' shared fixture does not: its PM-US ask is 0.99, so
    /// the inverted basket that side would open (sell Kalshi YES, buy PM YES)
    /// costs more than the dollar it pays, and the quoter declines for reasons
    /// that have nothing to do with suppression. Selling Kalshi YES at 0.98
    /// leaves 0.02 of NO against a PM YES at 0.20 — 0.22 for a dollar — which it
    /// quotes without hesitation.
    fn books_where_the_kalshi_ask_pays() -> BookBuilder {
        let lvl = |p: &str, s: &str| Level { price: p.into(), size: s.into() };
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.60", "500")], vec![lvl("0.99", "500")],
            1, 1_000_000_000, None);
        bb.apply_snapshot(Venue::PolymarketUs, "P", vec![lvl("0.10", "500")],
            vec![lvl("0.20", "500")], 1, 1_000_000_000, None);
        bb
    }

    /// Does the entry quoter still want to REST on the Kalshi ask?
    ///
    /// Asked through `on_book`, the public door, rather than through
    /// `Quoter::target`, which is private — and that is the better question
    /// anyway: what matters is whether a Place reaches the executor, not what an
    /// internal helper returned.
    fn quotes_kalshi_ask(quoters: &mut [Quoter], books: &BookBuilder) -> bool {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let (mut oid, mut intents) = (0u64, Vec::new());
        quoters[0].on_book(&mut cx, &fees, books, wall_now(), &mut oid, &mut intents);
        intents.iter().any(|i| match i {
            Intent::Place(p) => p.venue == Venue::Kalshi && p.side == BookSide::Ask,
            _ => false,
        })
    }

    /// THE VIEW IS THE MODULE'S ONLY INPUT, so publishing it is the whole of the
    /// wiring: without it `maker_exit::engine_view` fails closed and the loop
    /// decides nothing. A run without either flag must publish NOTHING, which is
    /// how an unflagged binary keeps the feature inert even if the loop were
    /// somehow spawned.
    #[test]
    fn the_view_is_published_only_when_a_flag_asked_for_it() {
        let _g = crate::maker_exit::test_serial();
        crate::maker_exit::reset_view();
        let (mut quoters, _) = quoter_and_books();

        let mut off = test_engine(test_cfg()); // maker_exit_view: false
        off.maker_exit_tick(&mut quoters);
        assert!(
            crate::maker_exit::engine_view().is_err(),
            "an unflagged engine must publish nothing"
        );

        let mut on = test_engine(armed_cfg());
        on.apr_bar = 14.5;
        on.maker_exit_tick(&mut quoters);
        let v = crate::maker_exit::engine_view().expect("published");
        assert_eq!(v.apr_bar, 14.5, "the hurdle in force, not a second derivation of it");
        crate::maker_exit::reset_view();
    }

    /// A HALTED ENGINE PUBLISHES NOTHING, AND THAT IS THE EXIT'S HALT PATH.
    ///
    /// `unwind_tick` states the rule — an exit is still an order, so a killed or
    /// feed-pulled engine stops deciding them — but the exit loop is a separate
    /// task and cannot read `self.killed`. Silence is how it is told: the view
    /// ages out, every decision refuses, and the loop PULLS what it has resting.
    #[test]
    fn a_killed_or_feed_pulled_engine_stops_publishing() {
        let _g = crate::maker_exit::test_serial();
        let (mut quoters, _) = quoter_and_books();
        for (killed, feed) in [(true, None), (false, Some("recorder stale".to_string()))] {
            crate::maker_exit::reset_view();
            let mut eng = test_engine(armed_cfg());
            eng.killed = killed;
            eng.feed_reason = feed.clone();
            eng.maker_exit_tick(&mut quoters);
            assert!(
                crate::maker_exit::engine_view().is_err(),
                "killed={killed} feed={feed:?} must go silent, not publish a stale hurdle"
            );
        }
        crate::maker_exit::reset_view();
    }

    /// THE ENTRY QUOTER YIELDS THE SIDE, AND THE OPERATOR'S OWN `--suppress`
    /// SURVIVES IT.
    ///
    /// `Quoter::set_suppress` REPLACES the whole set, so a tick that installed
    /// only the exit's pair would silently revoke every side another order-owner
    /// had declared — a quoter quoting into somebody else's book, caused by a
    /// feature that never mentions it.
    #[test]
    fn a_requested_side_is_yielded_without_dropping_the_operators_declaration() {
        let _g = crate::maker_exit::test_serial();
        crate::maker_exit::reset_view();
        let mut cfg = armed_cfg();
        cfg.suppress = [("OTHER".to_string(), BookSide::Bid)].into_iter().collect();

        // THE CONTROL FIRST, and it is not ceremony: `!quotes_kalshi_ask` is
        // satisfied by a fixture that never quotes that side at all, so without
        // this the suppression assertion below would pass against a quoter that
        // was silent for a completely unrelated reason.
        crate::maker_exit::request_suppress(std::collections::BTreeSet::new());
        let (mut quoters, _) = quoter_and_books();
        let books = books_where_the_kalshi_ask_pays();
        let mut eng = test_engine(cfg.clone_for_test());
        eng.maker_exit_tick(&mut quoters);
        assert!(
            quotes_kalshi_ask(&mut quoters, &books),
            "the fixture must quote the Kalshi ask, or the suppression test proves nothing"
        );

        // ...now ask for it, on a FRESH quoter: the entry quoter is a state
        // machine over its own resting orders, and reusing one that has already
        // been cancelled would test that machine rather than this seam.
        let (mut quoters, _) = quoter_and_books();
        let books = books_where_the_kalshi_ask_pays();
        let mut eng = test_engine(cfg);
        crate::maker_exit::request_suppress(["K".to_string()].into_iter().collect());
        eng.maker_exit_tick(&mut quoters);
        assert!(
            !quotes_kalshi_ask(&mut quoters, &books),
            "the exit's side must be yielded before anything rests on it"
        );
        // ...and the operator's own declaration survived the install, which
        // `set_suppress`'s replace-wholesale semantics make easy to lose.
        let v = crate::maker_exit::engine_view().expect("published");
        assert!(v.suppressed_at.contains_key("K"));
        let first = v.suppressed_at["K"];

        // A SECOND tick must not re-stamp it: the reader's settle window
        // measures how long the quoter has been OUT of the side, and a stamp
        // refreshed every 60s is a window that never elapses.
        std::thread::sleep(std::time::Duration::from_millis(5));
        eng.maker_exit_tick(&mut quoters);
        let v = crate::maker_exit::engine_view().expect("published");
        assert_eq!(v.suppressed_at["K"], first, "the install instant must not move");

        // ...and withdrawing the request releases it, so the settle clock
        // restarts rather than crediting an exit with a side it gave back.
        crate::maker_exit::request_suppress(std::collections::BTreeSet::new());
        eng.maker_exit_tick(&mut quoters);
        let v = crate::maker_exit::engine_view().expect("published");
        assert!(v.suppressed_at.is_empty(), "a withdrawn request is forgotten, not remembered");
        crate::maker_exit::reset_view();
    }

    /// THE PM-US ASK IS THE ONLY PRICE READ THE CLOSE LEG HAS, because
    /// `PmusGateway` has no `market_quote`. An empty ask side must be ABSENT
    /// rather than reported at zero: a close priced at $0.00 reads as free, and
    /// "no ask" spelled as a price is the same mistake `Quote::yes_bid`
    /// documents on the other venue.
    #[test]
    fn an_empty_pm_ask_side_is_absent_rather_than_a_price_of_zero() {
        let mut bb = BookBuilder::new();
        let lvl = |p: &str, s: &str| Level { price: p.into(), size: s.into() };
        bb.apply_snapshot(
            Venue::PolymarketUs, "P", vec![lvl("0.30", "500")], vec![lvl("0.34", "9")],
            1, 1_000_000_000, None,
        );
        bb.apply_snapshot(Venue::PolymarketUs, "DARK", vec![lvl("0.10", "5")], vec![],
            1, 1_000_000_000, None);
        // A Kalshi book must never leak into the PM map.
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.60", "5")], vec![lvl("0.61", "5")],
            1, 1_000_000_000, None);
        let asks = bb.pm_us_asks();
        assert_eq!(asks, vec![("P".to_string(), "0.34".to_string())], "{asks:?}");
    }
}

/// **The opportunistic-unwind scan.** Off by default, detect-only when on, and
/// silent while the engine is halted — see `Engine::unwind_tick`.
#[cfg(test)]
mod unwind_scan_tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("arb-trader-uw-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d.join(name)
    }

    /// A marks file stamped NOW, because `unwind_tick` ages it against the real
    /// wall clock (`bar_from_marks`) and a fixed timestamp would go stale.
    ///
    /// TWO POSITIONS, in the live book's shapes on 2026-07-29:
    ///   * `xvus-france-pres-27-…` x26 at forward 12.6%/yr, maker exit
    ///     +3.12c/ct — the one candidate the live marks actually select, and
    ///     one this engine's `--rel-prefix` scope does NOT cover;
    ///   * `xvus-france-pres-27-…` x30 at forward 13.8%/yr, maker exit
    ///     −4.97c/ct — the near-miss shape: under the hurdle, and getting out
    ///     costs more than it frees.
    fn marks_file(name: &str) -> String {
        write_marks(
            name,
            r#"{"relationship_id":"xvus-france-pres-27-mel",
                 "ts":1784646659.716,"kalshi_ticker":"KXFRENCHPRES-27-JMEL","qty":26,
                 "cost_usd":10.0,"locked_profit_usd":1.0,"resolves_by":"2027-04-25",
                 "resolves_estimated":true,
                 "forward_hold_apr":12.6,"maker_exit_ct":0.0312},
               {"relationship_id":"xvus-france-pres-27-hol",
                 "ts":1784726835.109,"kalshi_ticker":"KXFRENCHPRES-27-FHOL","qty":30,
                 "cost_usd":10.0,"locked_profit_usd":1.0,"resolves_by":"2027-04-25",
                 "resolves_estimated":true,
                 "forward_hold_apr":13.8,"maker_exit_ct":-0.0497}"#,
        )
    }

    /// A marks file stamped NOW around arbitrary positions, so a test can
    /// REWRITE the book under a live engine and watch what the next scan says.
    fn write_marks(name: &str, positions: &str) -> String {
        let now = wall_now();
        let s = (now as i64).rem_euclid(86_400);
        let stamp = format!(
            "{}T{:02}:{:02}:{:02}Z",
            crate::taketake::today_iso(now),
            s / 3600,
            (s % 3600) / 60,
            s % 60
        );
        let p = scratch(name);
        std::fs::write(&p, format!(r#"{{"generated_at":"{stamp}","positions":[{positions}]}}"#))
            .unwrap();
        p.to_string_lossy().into_owned()
    }

    /// A risk view whose class cap is a REAL number, which is what `select`
    /// refuses without. Read from a written file because `RiskView::load`
    /// answers a missing one with a $0 bankroll — and a $0 bankroll is exactly
    /// the degenerate cap under test in
    /// [`a_degenerate_global_cap_refuses_the_scan_instead_of_liquidating`].
    fn risk_with_cap(name: &str) -> Arc<crate::risk::RiskView> {
        let p = scratch(name);
        std::fs::write(&p, "bankroll_usd: 1000\nper_class_cap: 0.35\n").unwrap();
        Arc::new(crate::risk::RiskView::load(
            &p.to_string_lossy(),
            "/nonexistent/topics.yaml",
            vec![("kalshi".into(), "1000".into())],
            HashMap::new(),
        ))
    }

    fn engine_with_marks(name: &str) -> Engine {
        let mut cfg = test_cfg();
        // The live hurdle: a book at (or over) its class budget clamps
        // utilization to 1.0, so `apr_bar` is at its ceiling.
        cfg.apr_installed = (crate::APR_CEIL, "2026-07-29".into());
        cfg.risk = Some(risk_with_cap(&format!("{name}.exec.yaml")));
        // The armed unit's scope — which covers NEITHER position in the
        // fixture, exactly as it covers neither live candidate.
        cfg.unwind = Some(Unwind {
            marks_path: marks_file(name),
            owned_prefixes: vec!["xvus-nobel-peace-26".into()],
        });
        test_engine(cfg)
    }

    /// The scan finds the basket, counts the contracts it would free, and says
    /// so in gauges a monitor can read.
    ///
    /// AND IT REPORTS THAT IT CANNOT ACT ON IT. `unwind_candidates` non-zero
    /// with `unwind_actionable` pinned at 0 is the standing signal that the
    /// recycler and the engine are looking at different books — the thing that
    /// blocks arming, in one line of the stats tick.
    #[test]
    fn the_scan_reports_the_contracts_an_exit_would_free_and_whether_it_owns_them() {
        let mut eng = engine_with_marks("uw-found.json");
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 1, "12.6%/yr does not clear a 16%/yr hurdle");
        assert_eq!(eng.n_unwind_ct, 26);
        let s = eng.summary();
        assert_eq!(s["unwind_candidates"], serde_json::json!(1));
        assert_eq!(s["unwind_contracts"], serde_json::json!(26));
        assert_eq!(
            s["unwind_actionable"],
            serde_json::json!(0),
            "france-pres-27 is outside this process's --rel-prefix scope"
        );
    }

    /// **THE NEAR-MISS IS EMITTED.** The basket that cleared the APR test and
    /// failed only the exit floor is the most useful thing this scan produces,
    /// and it used to be dropped on the floor (`let Ok((exits, _skips))`), so
    /// all five skip causes rendered as `unwind_candidates: 0`.
    #[test]
    fn the_baskets_that_almost_cleared_reach_the_stats_line() {
        let mut eng = engine_with_marks("uw-nearmiss.json");
        eng.unwind_tick();
        let s = eng.summary();
        assert_eq!(s["unwind_near_miss"], serde_json::json!(1), "{s}");
        // 30 x -0.0497 = -1.491: what forcing that one out would cost.
        let usd = s["unwind_near_miss_usd"].as_f64().unwrap();
        assert!((usd - -1.491).abs() < 1e-9, "{usd}");
    }

    /// **A SCAN THAT SELECTS NOTHING STILL HAS TO SAY WHY — AND THAT IS THE
    /// LIVE CASE.** No candidate selects on today's marks at either the old
    /// half-cent floor or the new two-tick one, so this is the ONLY path a
    /// freshly-flagged process takes.
    ///
    /// It used to take it in total silence. `unwind_seen` started `Vec::new()`
    /// and `unwind::identity_set(&[])` is `[]`, so the edge-trigger compared
    /// EQUAL on the first tick and on every tick after: the "nothing to exit"
    /// line and the `[unwind] skipped:` breakdown were unreachable, and a book
    /// whose entire contents flipped from hold-is-better to unpriceable printed
    /// nothing at all. A detect-only feature that detects silently has no
    /// deliverable.
    #[test]
    fn a_book_that_selects_no_candidate_still_reports_its_skip_breakdown() {
        let mut cfg = test_cfg();
        cfg.apr_installed = (crate::APR_CEIL, "2026-07-29".into());
        cfg.risk = Some(risk_with_cap("uw-quiet.exec.yaml"));
        // Every position holds: forward 22.2%/yr clears the ceiling hurdle.
        cfg.unwind = Some(Unwind {
            marks_path: write_marks(
                "uw-quiet.json",
                r#"{"relationship_id":"xvus-nobel-peace-26-a","ts":1.0,
                     "kalshi_ticker":"KX-A","qty":10,"resolves_by":"2027-04-25",
                     "forward_hold_apr":22.2,"maker_exit_ct":0.30}"#,
            ),
            owned_prefixes: Vec::new(),
        });
        let mut eng = test_engine(cfg);

        let first = eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0, "22.2%/yr beats the hurdle — nothing to exit");
        assert!(
            first.iter().any(|l| l.contains("nothing to exit") && l.contains("hold-is-better 1")),
            "the FIRST scan of an empty candidate set has to speak: {first:?}"
        );

        // ...and it does not repeat itself while the book does not change.
        assert!(eng.unwind_tick().is_empty(), "an unchanged book is not news");

        // ...but the SAME empty candidate set arrived at for a different reason
        // is a different finding, and the trigger keyed on candidates alone
        // could not see it: `[]` before, `[]` after.
        write_marks(
            "uw-quiet.json",
            r#"{"relationship_id":"xvus-nobel-peace-26-a","ts":1.0,
                 "kalshi_ticker":"KX-A","qty":10,"resolves_by":"2027-04-25",
                 "forward_hold_apr":null,"maker_exit_ct":null}"#,
        );
        let flipped = eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0, "still nothing to exit");
        assert!(
            flipped.iter().any(|l| l.contains("unpriceable 1")),
            "a book the marks writer has gone dark on is not the same silence: {flipped:?}"
        );
    }

    /// **A DEGENERATE CAP REFUSES THE SCAN.** `utilization()` answers a
    /// cap of zero with `1.0`, which is fail-closed for an entry and fail-OPEN
    /// for an exit: it pins the liquidation hurdle at its ceiling. A missing or
    /// corrupt `exec.yaml` — or no risk view at all — must therefore select
    /// NOTHING and name the reason, not liquidate the book.
    #[test]
    fn a_degenerate_global_cap_refuses_the_scan_instead_of_liquidating() {
        let mut eng = engine_with_marks("uw-cap.json");
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 1, "control: a real cap selects");
        assert_eq!(
            eng.summary()["unwind_refused"],
            serde_json::json!(null),
            "control: null means the scan RAN"
        );

        // `RiskView::load` on a file that is not there: bankroll 0, cap 0.
        eng.cfg.risk = Some(Arc::new(crate::risk::RiskView::load(
            "/nonexistent/exec.yaml",
            "/nonexistent/topics.yaml",
            vec![("kalshi".into(), "1000".into())],
            HashMap::new(),
        )));
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0, "a fabricated utilization decides nothing");
        assert!(
            eng.unwind_refused.as_deref().unwrap_or_default().contains("global cap"),
            "and it says which input: {:?}",
            eng.unwind_refused
        );
        // ...AND THE MONITORS CAN SEE IT. Every unwind gauge now reads zero,
        // exactly as it does on a converged book; the reason is the only thing
        // that tells the two apart, and PR #46 established that the monitors
        // read this JSON and not stderr.
        let s = eng.summary();
        assert_eq!(s["unwind_candidates"], serde_json::json!(0), "{s}");
        assert!(
            s["unwind_refused"].as_str().unwrap_or_default().contains("global cap"),
            "a refusal that is invisible in the summary is a subsystem that went quiet: {s}"
        );

        // ...and no risk view at all is the same refusal for the same reason.
        eng.cfg.risk = None;
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0);
        assert!(eng.unwind_refused.is_some());
        assert!(eng.summary()["unwind_refused"].is_string());
    }

    /// **OFF BY DEFAULT.** `cfg.unwind: None` is not "scan and find nothing" —
    /// the scan does not run at all, which is what makes this a flag rather
    /// than a behaviour change to every existing run.
    #[test]
    fn the_scan_does_not_run_unless_it_is_asked_for() {
        let mut cfg = test_cfg();
        cfg.apr_installed = (crate::APR_CEIL, "2026-07-29".into());
        cfg.risk = Some(risk_with_cap("uw-off.exec.yaml"));
        cfg.unwind = None; // ...but the same marks are on disk
        let _ = marks_file("uw-off.json");
        let mut eng = test_engine(cfg);
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0);
        assert_eq!(eng.summary()["unwind_candidates"], serde_json::json!(0));
    }

    /// **AN UNWIND IS NOT EXEMPT FROM THE HALT PATH.** A killed engine has
    /// cancelled its book and is trying to stop; a feed-pulled one cannot see
    /// the prices it would quote against. An exit is still an order, so it
    /// stops on both — and the gate lives with the DECISION so that the placer
    /// this is the first half of cannot be written without it.
    #[test]
    fn a_halted_engine_selects_nothing_to_unwind() {
        let mut eng = engine_with_marks("uw-halted.json");
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 1, "control: the candidate is there");

        eng.killed = true;
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0, "the kill switch stops the unwind too");
        assert_eq!(eng.n_unwind_ct, 0);

        eng.killed = false;
        eng.feed_reason = Some("stale feed".into());
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 0, "so does the feed-stale pull");

        eng.feed_reason = None;
        eng.unwind_tick();
        assert_eq!(eng.n_unwind, 1, "and it comes back when the halt clears");
    }

    /// A marks file the scan cannot trust decides NOTHING, and says which.
    /// `unwind_candidates: 0` alone would read identically to a converged book.
    #[test]
    fn unusable_marks_refuse_rather_than_reporting_an_empty_book() {
        let mut cfg = test_cfg();
        cfg.apr_installed = (crate::APR_CEIL, "2026-07-29".into());
        cfg.risk = Some(risk_with_cap("uw-torn.exec.yaml"));
        cfg.unwind = Some(Unwind {
            marks_path: "/nonexistent/marks.json".into(),
            owned_prefixes: Vec::new(),
        });
        let mut eng = test_engine(cfg);
        eng.unwind_tick();
        assert!(eng.unwind_refused.is_none(), "an ABSENT file is a cold start, not a fault");

        let torn = scratch("uw-torn.json");
        std::fs::write(&torn, r#"{"generated_at":"2026-07-2"#).unwrap();
        eng.cfg.unwind = Some(Unwind {
            marks_path: torn.to_string_lossy().into_owned(),
            owned_prefixes: Vec::new(),
        });
        eng.unwind_tick();
        assert!(eng.unwind_refused.is_some(), "a torn write is a fault and must be named");
        assert_eq!(eng.n_unwind, 0);
    }
}

/// In-process marking, WIRED — the half `crate::marks` cannot test.
///
/// That module tests the arithmetic and the schema against a pure function.
/// What is only decidable here is WHEN a mark is rebuilt: which book event arms
/// it, which one does not, and the two bounds that keep the rewrite off the
/// decision path without letting `generated_at` age out of
/// `taketake::MAX_MARKS_AGE_S`.
#[cfg(test)]
mod marks_wiring_tests {
    use super::*;
    use serde_json::json;

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("arb-trader-marks-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d.join(name)
    }

    /// One open basket on `K` / `P`, with a cost basis so it produces a row.
    fn ledger(name: &str) -> String {
        let p = scratch(name);
        std::fs::write(
            &p,
            "{\"ts\":1785402005.539014,\"relationship_id\":\"xvus-nobel-peace-26-b4b\",\
             \"qty\":1,\"status\":\"open\",\"cost_usd\":0.72,\"profit_usd\":0.28,\
             \"legs\":[{\"venue\":\"kalshi\",\"market_id\":\"K\",\"side\":\"yes\"},\
             {\"venue\":\"polymarket_us\",\"market_id\":\"P\",\"side\":\"no\"}]}\n",
        )
        .unwrap();
        p.to_string_lossy().into_owned()
    }

    fn marks_cfg(tag: &str, min_interval_s: f64, max_idle_s: f64) -> (RunCfg, String) {
        let out = scratch(&format!("{tag}-marks.json"));
        let out_path = out.to_string_lossy().into_owned();
        let cfg = RunCfg {
            marks_out: Some(MarksOut {
                out_path: out_path.clone(),
                ledger_path: ledger(&format!("{tag}-trades.jsonl")),
                min_interval_s,
                max_idle_s,
            }),
            ..test_cfg()
        };
        (cfg, out_path)
    }

    fn snapshot(venue: &str, market: &str) -> String {
        json!({"kind": "snapshot", "venue": venue, "market_id": market,
               "bids": [{"price": "0.05", "size": "50"}],
               "asks": [{"price": "0.08", "size": "50"}],
               "seq": 1, "ts_local_ns": 1_785_402_100_000_000_000i64})
        .to_string()
    }

    fn feed(eng: &mut Engine, line: &str) {
        eng.on_feed_line(line, &mut [], &HashMap::new());
    }

    /// **THE TRIGGER.** A book event on a market an open basket holds arms the
    /// re-mark; one on any other market does not.
    ///
    /// This is the whole difference between this and `arbbot-marks.timer`. The
    /// engine sees ~250 book events a second and holds legs on 14 markets, so
    /// an unfiltered trigger would rewrite a 15 KB file at feed rate on a box
    /// whose recorder stalls under I/O — and a trigger that missed the held
    /// markets would leave the file frozen at whatever the heartbeat last wrote.
    #[test]
    fn a_book_event_on_a_held_market_arms_the_remark_and_nothing_else_does() {
        let (cfg, _) = marks_cfg("trigger", 0.0, 0.0);
        let mut eng = test_engine(cfg);
        // The first tick loads the ledger — and so builds the watch set — but
        // writes nothing, because no book has arrived yet.
        eng.marks_tick();
        assert_eq!(eng.n_marks, 0, "a process that has not seen a book has nothing to mark");

        feed(&mut eng, &snapshot("kalshi", "SOMETHING-ELSE"));
        assert!(!eng.marks_dirty, "a market no basket holds is not a re-mark");
        feed(&mut eng, &snapshot("polymarket_us", "P"));
        assert!(eng.marks_dirty, "a market a basket DOES hold is");
        eng.marks_tick();
        assert_eq!(eng.n_marks, 1, "and the tick spends it");
        assert!(!eng.marks_dirty, "clearing the bit");

        // ...and the Kalshi leg arms it just the same, which the live book needs:
        // its Kalshi legs tick and its PM-US legs sometimes are not carried at all.
        feed(&mut eng, &snapshot("kalshi", "K"));
        assert!(eng.marks_dirty);
    }

    /// **A RESTART DOES NOT BLANK THE FILE IT INHERITS.**
    ///
    /// The first deadline is due immediately and the welcome snapshot burst
    /// takes about a second to arrive, so a marking engine that wrote on that
    /// first tick would replace a perfectly good marks file with one where every
    /// row is unpriced — and, worse, report every held market as uncarried. Seen
    /// live on 2026-07-31 before this guard existed.
    ///
    /// It lifts on the first book event, not on a timer, so an engine whose feed
    /// never comes up never writes at all — which is the correct answer: it has
    /// nothing to say about prices it has not seen.
    #[test]
    fn nothing_is_written_until_a_book_has_actually_arrived() {
        let (cfg, out) = marks_cfg("cold", 0.0, 0.0);
        let mut eng = test_engine(cfg);
        for _ in 0..5 {
            eng.marks_tick();
        }
        assert_eq!(eng.n_marks, 0, "five ticks and no feed is still nothing to say");
        assert!(!std::path::Path::new(&out).exists(), "and the inherited file is untouched");
        assert!(eng.marks_no_book.is_empty(), "nothing is reported as uncarried either");

        feed(&mut eng, &snapshot("kalshi", "K"));
        eng.marks_tick();
        assert_eq!(eng.n_marks, 1, "the first book lifts it");
    }

    /// **THE TWO BOUNDS.** A quiet book still gets a heartbeat; a busy one does
    /// not get a rewrite per event.
    ///
    /// The heartbeat is not cosmetic: `generated_at` is what
    /// `taketake::MAX_MARKS_AGE_S` ages, so a book that goes quiet for 900
    /// seconds with no heartbeat would age out the engine's OWN take-take bar
    /// and refuse take-take entirely — a marking loop that switched trading off
    /// by being idle.
    #[test]
    fn the_write_is_floored_by_the_interval_and_forced_by_the_heartbeat() {
        // A floor of an hour, a heartbeat of a day: nothing may write twice.
        // A floor of an hour: the first write lands, the second cannot.
        let (cfg, _) = marks_cfg("floor", 3600.0, 86_400.0);
        let mut eng = test_engine(cfg);
        eng.marks_tick(); // loads the ledger; no book yet
        feed(&mut eng, &snapshot("kalshi", "K"));
        eng.marks_tick();
        assert_eq!(eng.n_marks, 1);
        feed(&mut eng, &snapshot("kalshi", "K"));
        assert!(eng.marks_dirty, "the book moved again");
        eng.marks_tick();
        assert_eq!(eng.n_marks, 1, "...and the floor refused the rewrite");
        assert!(eng.marks_dirty, "the trigger is REMEMBERED, not spent");

        // No floor, no idle allowance, and NOTHING dirty: the heartbeat writes
        // anyway, which is the case that keeps the bar alive on a dead book.
        let (cfg, out) = marks_cfg("beat", 0.0, 0.0);
        let mut eng = test_engine(cfg);
        feed(&mut eng, &snapshot("kalshi", "K"));
        eng.marks_dirty = false;
        eng.marks_tick();
        eng.marks_tick();
        assert_eq!(eng.n_marks, 2, "not dirty, and still written twice");
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert_eq!(doc["totals"]["n_open"], json!(1));
        assert_eq!(doc["positions"].as_array().unwrap().len(), 1);
    }

    /// A HALTED engine keeps marking. `unwind_tick` stops under the same
    /// conditions because an exit is an order; marking sends nothing, and the
    /// halted process is the only thing left that can keep the marks — and so
    /// the bar derived from them — current.
    #[test]
    fn a_halted_engine_still_marks() {
        let (cfg, _) = marks_cfg("halted", 0.0, 0.0);
        let mut eng = test_engine(cfg);
        feed(&mut eng, &snapshot("kalshi", "K"));
        eng.killed = true;
        eng.feed_reason = Some("FEED DOWN".into());
        eng.marks_tick();
        assert_eq!(eng.n_marks, 1, "a halt silences orders, not marks");
    }

    /// A write that cannot happen is NAMED in the summary, not swallowed.
    ///
    /// This file is an input to the engine's own take-take bar. A marking path
    /// that has silently stopped writing is the 2026-07-28 incident with the
    /// blame moved in-process, and `marks_written` frozen beside a null
    /// `marks_error` would say nothing at all about it.
    #[test]
    fn a_write_that_fails_is_reported_rather_than_swallowed() {
        let (mut cfg, _) = marks_cfg("failing", 0.0, 0.0);
        // /proc exists and is writable by nobody, so the path is well-formed
        // and the write cannot succeed.
        cfg.marks_out.as_mut().unwrap().out_path = "/proc/marks.json".into();
        let mut eng = test_engine(cfg);
        feed(&mut eng, &snapshot("kalshi", "K"));
        eng.marks_tick();
        assert_eq!(eng.n_marks, 0, "a failed write is not a write");
        let s = eng.summary();
        assert_eq!(s["marks_written"], json!(0));
        assert!(
            s["marks_error"].as_str().unwrap_or_default().contains("/proc/marks"),
            "the reason a monitor reads: {s}"
        );
    }

    /// The coverage gap, wired: a held market the engine has no book for leaves
    /// its row UNPRICED and says which market by name.
    ///
    /// On 2026-07-31 this is the live state for every `xvus-france-pres-27`
    /// basket — 8 of 25 rows — because the recorder's PM-US universe is
    /// tag-driven and those two slugs are not in it. `mark_positions.py` prices
    /// them over REST, so this is a REGRESSION against the writer being
    /// retired, and it has to be visible in the summary a monitor reads.
    #[test]
    fn a_held_market_with_no_book_is_counted_and_named() {
        let (cfg, _) = marks_cfg("nobook", 0.0, 0.0);
        let mut eng = test_engine(cfg);
        // Kalshi arrives; the PM-US leg never does.
        feed(&mut eng, &snapshot("kalshi", "K"));
        eng.marks_tick();
        let s = eng.summary();
        assert_eq!(s["marks_unpriced_rows"], json!(1), "the row is published, unpriced: {s}");
        assert_eq!(s["marks_no_book"], json!(1), "and the market is counted: {s}");
        assert!(eng.marks_no_book.contains("polymarket_us:P"), "by name: {:?}", eng.marks_no_book);

        // ...and once it does arrive, both go back to zero.
        feed(&mut eng, &snapshot("polymarket_us", "P"));
        eng.marks_tick();
        let s = eng.summary();
        assert_eq!(s["marks_unpriced_rows"], json!(0), "{s}");
        assert_eq!(s["marks_no_book"], json!(0), "{s}");
        assert!(eng.marks_no_book.is_empty());
    }
}
