//! Venue-truth positions reconciliation. OFF unless `--positions-recon` is
//! passed, and DETECTION-ONLY unless `--positions-recon-act` arms it as well.
//!
//! THE GAP THIS CLOSES. The engine hedges reactively off its OWN fills, so
//! everything it knows about its exposure is downstream of a fill frame it
//! attributed. A position that arrived some other way is invisible to it: a
//! fill frame that never came (`fills::kalshi_fill_gaps`), one that came and
//! could not be matched to an order (`fills_unattributed` — 9 of them on
//! 2026-07-30), an obligation a previous process minted and forgot
//! (`orphan`), or a leg some other owner has since CLOSED. That last one is
//! not hypothetical either: on 2026-07-30 this engine reported `hedges_naked:
//! 1` for a leg `arbbot-hedge.timer` had already hedged, because nothing it
//! reads could tell it the position was gone.
//!
//! `fills.rs` states the delegation in as many words — the terminal case "is
//! left to arbbot-hedge.timer's venue-truth read ... which covers both venues"
//! — and `scripts/hedge_naked_legs.py` is that read: every 5 minutes it pulls
//! ACTUAL POSITIONS from both venues and finds nakedness however it arose. That
//! script is frozen Python. This is the same read, in the engine.
//!
//! WHAT IT IS NOT, UNDER `--positions-recon` ALONE. It does not place, size,
//! price or book anything, and it does not feed the hedge path. Two hedgers on
//! one Kalshi key is not redundancy, it is a DOUBLE HEDGE — `orphan.rs` works
//! that through: both IOCs fill, the account ends long 2 against a short 1, and
//! `hedges_overfilled` cannot even see it because the other party's fill is
//! credited to no obligation of ours.
//!
//! THE ARMED SPELLING IS `--positions-recon-act`, and it is the reviewed change
//! the paragraph above deferred to — the one that lets this REPLACE
//! `arbbot-hedge.timer` rather than join it. Everything it decides lives in
//! `crate::naked_act`, which is also where the reasons are; the cutover
//! sequencing (stop the timer FIRST) is an operator's and cannot be enforced
//! from here. `--positions-recon` on its own is unchanged, down to the byte:
//! `act` is `None`, the gauges move exactly as they did, and the only code that
//! runs is the read.
//!
//! WHY A SNAPSHOT DIFFERENCE IS SOUND HERE AND NOT IN `orphan`. That module
//! rejects venue positions for recovering hedge OBLIGATIONS and the four
//! reasons it gives are correct — but three of them are about attribution and
//! action, not detection. It needs an anchor price and an age per obligation to
//! decide a hedge; this needs neither, because it decides nothing. What
//! survives as a real objection is the first: a bad read reads exactly like a
//! naked leg. That one is answered guard by guard below, and it is the whole
//! design.
//!
//! THE GUARDS, and what each is for. A wrong "naked" here is a false alarm
//! today and an unnecessary REAL ORDER the day anything is wired to act on it,
//! so each transient failure mode this endpoint pair is documented to have gets
//! a named answer:
//!
//!   1. EMPTY READ (`pmus-positions-empty-glitch`). Refused at the gateway and
//!      retried here — `PmusGateway::net_positions` errors rather than
//!      answering an empty map, exactly as `PmusSession.get_positions` raises
//!      unless `allow_empty`. Mirrors `_retry` in `hedge_naked_legs.py:41`.
//!   2. DROPPED ROWS (same quirk family). Two consecutive reads must agree
//!      EXACTLY on the net map before either is used — the port of
//!      `pmus_positions_consensus` (`hedge_naked_legs.py:74`), 4 attempts, 2s
//!      apart. A row present in one read and absent from the next is noise, and
//!      the run refuses rather than picking one.
//!   3. STICKY STALENESS (`pmus-positions-partial-stale-sticky`). A server-side
//!      cache survives back-to-back reads, so guard 2 cannot see it — the quirk
//!      says so outright. The answer is separated-in-time reads: an imbalance
//!      is reported only when the SAME imbalance appears on two consecutive
//!      cycles, 5 minutes apart. That is `reconcile_positions.py:215-235`'s
//!      "two consecutive RUNS" rule, and it is the one guard here that is
//!      stronger than the 5-minute timer's, which cannot remember its last run
//!      without a database.
//!   4. TRUNCATION. Both `net_positions` refuse a partial map rather than let
//!      absent rows read as zero.
//!   5. A FAILED READ IS NOT AN ANSWER. Any refusal — 503, rate budget, parse,
//!      unstable consensus — abandons the cycle, leaves every gauge as it was,
//!      and counts `positions_recon_failures`. It never concludes "no positions
//!      found, therefore nothing is naked". Because a stuck reconciliation
//!      reporting a stale `positions_recon_naked: 0` is itself a way to be
//!      wrong, `positions_recon_age_s` says how old that number is, and it is
//!      the gauge to alarm on.
//!
//! Guard 3 also happens to answer the one thing a positions snapshot genuinely
//! cannot do (`orphan.rs`'s third objection): it "cannot tell a three-hour-old
//! orphan from a leg that filled 200ms ago whose partner is still on the wire".
//! A leg that is naked for less than a cycle is never reported, and the ordinary
//! hedge round trip is milliseconds.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Market id -> signed contract count, one venue.
pub type NetMap = BTreeMap<String, f64>;

/// How often the cycle runs — the same 5 minutes as `arbbot-hedge.timer`, and
/// therefore also the confirmation delay imposed by guard 3.
const INTERVAL: Duration = Duration::from_secs(300);
/// `_retry(tries=3, delay=0.8)` from `hedge_naked_legs.py:41`, backing off the
/// same way (0.8s, then 1.6s).
const RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(800);
/// `pmus_positions_consensus(attempts=4)`, 2s apart.
const CONSENSUS_ATTEMPTS: u32 = 4;
const CONSENSUS_GAP: Duration = Duration::from_secs(2);

// ------------------------------------------------------------------ gauges ---

/// Naked legs CONFIRMED by the last completed cycle. Read it next to
/// `positions_recon_age_s`: a 0 from a cycle that last completed an hour ago is
/// not a clean book, it is a broken reconciliation.
static NAKED: AtomicI64 = AtomicI64::new(0);
/// Imbalances SEEN by the last completed cycle but not yet confirmed by a
/// second one. Normal transient state; a number that never turns into a
/// confirmation is the venue noise guard 3 exists to absorb, and it is
/// reported so that absorbing it is visible rather than silent.
static UNCONFIRMED: AtomicI64 = AtomicI64::new(0);
/// Cycles abandoned because a venue read could not be trusted. Not
/// must-stay-0: one PM-US 503 (they came in runs on 2026-07-31) or one
/// disagreeing consensus is exactly what this counter is for. A count that
/// keeps climbing while `positions_recon_age_s` climbs with it is the
/// reconciliation being DOWN, which is the posture this module exists to end.
static FAILURES: AtomicU64 = AtomicU64::new(0);
/// Unix seconds of the last COMPLETED cycle; 0 = never completed one.
static LAST_OK_S: AtomicI64 = AtomicI64::new(0);
/// Orders this process has PLACED against a confirmed naked leg and seen fill
/// (`--positions-recon-act` only; 0 forever without it). Real money.
static ACTED: AtomicU64 = AtomicU64::new(0);
/// Confirmed findings the act pass declined. NOT an error count — declining is
/// the normal state, because the policy is to wait for a book that pays rather
/// than to chase one. It is the number to read next to `positions_recon_naked`:
/// legs confirmed but never acted on are legs the guards are holding, and the
/// journal says which guard for each.
static ACT_REFUSED: AtomicU64 = AtomicU64::new(0);
/// Orders the venue ACCEPTED and whose fate this process could not determine.
///
/// MUST STAY 0, and it is the only counter here of which that is true. Every
/// other refusal above is a non-event — no order was sent, or one was sent and
/// demonstrably did not trade. This one is an open question about real money:
/// the contracts may be in the account, and they are certainly not in the
/// ledger, which is the one state no exposure fold, no risk cap and no unwind
/// can see. It is deliberately NOT folded into `positions_recon_act_refused`,
/// because "the book did not pay" and "we do not know what we own" must never
/// share a number.
static ACT_UNRESOLVED: AtomicU64 = AtomicU64::new(0);

pub fn naked() -> i64 {
    NAKED.load(Ordering::Relaxed)
}
pub fn unconfirmed() -> i64 {
    UNCONFIRMED.load(Ordering::Relaxed)
}
pub fn failures() -> u64 {
    FAILURES.load(Ordering::Relaxed)
}
pub fn acted() -> u64 {
    ACTED.load(Ordering::Relaxed)
}
pub fn act_refused() -> u64 {
    ACT_REFUSED.load(Ordering::Relaxed)
}
pub fn act_unresolved() -> u64 {
    ACT_UNRESOLVED.load(Ordering::Relaxed)
}

/// Seconds since the last completed cycle. `-1` = never completed one, which
/// includes "not enabled" — a distinct value rather than a large number,
/// because "no reading yet" and "a very old reading" call for different
/// responses.
pub fn age_s() -> i64 {
    match LAST_OK_S.load(Ordering::Relaxed) {
        0 => -1,
        t => (now_s() - t).max(0),
    }
}

fn now_s() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ------------------------------------------------------------------- model ---

/// A relationship reduced to the two market ids a position read is keyed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    pub rel_id: String,
    pub kalshi: String,
    pub pmus: String,
}

/// Which leg is exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    /// PM-US is shorter than Kalshi is long: contracts sold with nothing
    /// behind them. `qty` is what completing the basket would cost, and it is
    /// the case `hedge_naked_legs.py` acts on.
    PmShort,
    /// Kalshi is longer than PM-US is short. Alerted and NOT auto-hedged by
    /// the Python ("not auto-hedged (v1)"); the policy for it belongs to
    /// whoever wires action, not here.
    KalshiLong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub rel_id: String,
    pub kalshi: String,
    pub pmus: String,
    /// Kalshi's signed contract count for this leg.
    pub kq: f64,
    /// PM-US's signed contract count for this leg.
    pub pq: f64,
    /// `kq + pq` — zero for a hedged basket, because a basket holds one leg
    /// long and the other short.
    pub imb: f64,
    pub leg: Leg,
    /// Whole contracts exposed, `|imb|` rounded.
    pub qty: i64,
}

impl Finding {
    /// Cross-cycle identity. It carries BOTH quantities, not just the
    /// relationship, so a moving imbalance never confirms itself: two cycles
    /// that disagree about the size are two different observations, and the
    /// one thing this must not do is average them into an alarm. The port of
    /// `reconcile_positions.py`'s sighting key, which is the same string down
    /// to the rounding.
    pub fn key(&self) -> String {
        format!(
            "{}:k{:+.0}/pm{:+.0}/imb{:+.0}",
            self.rel_id, self.kq, self.pq, self.imb
        )
    }

    /// Both quantities and the imbalance, always — the leg name alone has
    /// never been enough to tell a real one from a bad read at 3am.
    pub fn line(&self) -> String {
        let what = match self.leg {
            Leg::PmShort => "PM-SHORT NAKED",
            Leg::KalshiLong => "KALSHI-LONG NAKED",
        };
        format!(
            "{} {what} x{} (kalshi {} {:+.0}, pmus {} {:+.0}, imb {:+.2})",
            self.rel_id, self.qty, self.kalshi, self.kq, self.pmus, self.pq, self.imb
        )
    }
}

/// Imbalances in one snapshot pair.
///
/// The derivation is `hedge_naked_legs.py:168-178` line for line, including the
/// two things about it that look like bugs and are not:
///
///   * A PM slug ABSENT from the read is SKIPPED, not read as zero
///     (`if ps not in ppos: continue`). This is what makes a dropped row a
///     missed detection instead of a false alarm, and it is the right way
///     round.
///   * The deadband is ASYMMETRIC: naked below -0.5, alert at or above +1,
///     nothing between. It is not a rounding artifact — it is the threshold a
///     real order is placed against, so it is mirrored exactly rather than
///     tidied. `reconcile_positions.py` uses a symmetric `abs(imb) >= 1` for
///     its alert, so the two Python readers genuinely disagree on
///     `-1 < imb <= -0.5`, where this one reports and that one does not. This
///     follows the actioning script, and the difference is called out here
///     rather than silently resolved.
///
/// `excluded` is the seam for the ownership predicate: markets another
/// order-owner is working are not ours to call naked. `hedge_naked_legs.py`
/// has one already (`probe_owned_slugs`, the `owner 'probe-'` table plus a
/// transitional grep of the probe logs), and a sibling change is porting it.
/// Until it lands the caller passes "exclude nothing", which is the WIDE
/// setting: it can over-report, never under-report, and this pass only
/// reports.
pub fn find(
    pairs: &[Pair],
    kpos: &NetMap,
    ppos: &NetMap,
    excluded: &dyn Fn(&Pair) -> bool,
) -> Vec<Finding> {
    let mut out = Vec::new();
    // Dedupe on the MARKET ids, not the relationship id: two registry entries
    // naming the same two markets are one position to reconcile, and reporting
    // it twice would double `positions_recon_naked`
    // (`reconcile_positions.py`'s `seen` set).
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    for p in pairs {
        let Some(&pq) = ppos.get(&p.pmus) else { continue };
        if excluded(p) {
            continue;
        }
        if !seen.insert((p.kalshi.clone(), p.pmus.clone())) {
            continue;
        }
        let kq = kpos.get(&p.kalshi).copied().unwrap_or(0.0);
        let imb = kq + pq;
        let leg = if imb < -0.5 {
            Leg::PmShort
        } else if imb >= 1.0 {
            Leg::KalshiLong
        } else {
            continue;
        };
        out.push(Finding {
            rel_id: p.rel_id.clone(),
            kalshi: p.kalshi.clone(),
            pmus: p.pmus.clone(),
            kq,
            pq,
            imb,
            leg,
            qty: imb.abs().round() as i64,
        });
    }
    out
}

/// The cross-cycle confirmation state (guard 3).
pub struct Recon {
    pairs: Vec<Pair>,
    /// Finding keys from the previous COMPLETED cycle. A refused cycle does
    /// not touch this: it neither confirms nor forgets, because a read we could
    /// not trust is not evidence in either direction.
    last: BTreeSet<String>,
}

impl Recon {
    pub fn new(pairs: Vec<Pair>) -> Recon {
        Recon { pairs, last: BTreeSet::new() }
    }

    pub fn pairs(&self) -> &[Pair] {
        &self.pairs
    }

    /// One cycle over a trusted snapshot. Returns `(confirmed, unconfirmed)`:
    /// findings that also appeared last cycle, and the rest.
    ///
    /// The first cycle of a process therefore confirms nothing, by
    /// construction. That is the cost of guard 3 and it is worth stating: a leg
    /// that is genuinely naked at startup is reported at the SECOND cycle, five
    /// minutes in, not the first.
    ///
    /// PURE, and publishes no gauge: only `cycle` does that, and only over a
    /// snapshot every guard has already passed. Keeping the two apart is what
    /// makes "a failed read moves nothing" a property of one function rather
    /// than a discipline spread over several.
    pub fn step(
        &mut self,
        kpos: &NetMap,
        ppos: &NetMap,
        excluded: &dyn Fn(&Pair) -> bool,
    ) -> (Vec<Finding>, Vec<Finding>) {
        let found = find(&self.pairs, kpos, ppos, excluded);
        let keys: BTreeSet<String> = found.iter().map(|f| f.key()).collect();
        let (confirmed, fresh): (Vec<Finding>, Vec<Finding>) =
            found.into_iter().partition(|f| self.last.contains(&f.key()));
        self.last = keys;
        (confirmed, fresh)
    }
}

/// Every registry relationship with both a Kalshi and a PM-US leg
/// (`hedge_naked_legs.py:151-155`).
///
/// The registry is read ONCE, at spawn. A pair added while the process runs is
/// not reconciled until it restarts — acceptable because the same is already
/// true of the quoters, which load their universe at startup from the same
/// file.
///
/// NOT COVERED, and this is a real gap rather than a simplification: the Python
/// also unions in the generic sports pairings from
/// `data/scan/sports_equiv_map.json`, and `reconcile_positions.py` additionally
/// derives MLB pairs from the Kalshi ticker by regex. Sports legs are
/// reconciled by the timer and are NOT reconciled here. They are a missed
/// detection, never a false one.
pub fn pairs_from_registry(path: &str) -> Vec<Pair> {
    use arb_core::model::Venue;
    let Ok(reg) = arb_registry::Registry::load(path) else { return Vec::new() };
    // `Venue::parse`, not a string compare: it documents itself as the ONLY
    // inverse of `as_str` because six hand-written copies of that match once
    // lived in six files, and a venue spelled some other way parses as `None`
    // at every one of them — which here would silently drop a pair from the
    // reconciliation and read as "nothing naked".
    let leg = |r: &arb_registry::Relationship, v: Venue| {
        r.legs
            .iter()
            .find(|l| Venue::parse(&l.venue) == Some(v))
            .map(|l| l.market_id.clone())
    };
    let mut out = Vec::new();
    for r in reg.relationships {
        if let (Some(kalshi), Some(pmus)) =
            (leg(&r, Venue::Kalshi), leg(&r, Venue::PolymarketUs))
        {
            out.push(Pair { rel_id: r.id, kalshi, pmus });
        }
    }
    out
}

// ---------------------------------------------------------------------- io ---

/// Read one venue's net positions, retrying a refusal.
///
/// Blocking sink calls go through `spawn_blocking` for the same reason
/// `fills::reconcile` does: the sink is synchronous and the engine's runtime is
/// not somewhere to park a venue round trip.
///
/// SEAM (sibling change): every failure is treated identically here — retried
/// `RETRIES` times, then the cycle is abandoned. A `VenueError` classifier that
/// separates retryable (503, timeout, rate budget) from terminal (401, a
/// changed payload shape, `NotWired`) belongs at this call, and terminal ones
/// should stop retrying and back off rather than spend three attempts. Nothing
/// here is unsafe without it — the failure path already refuses — it just
/// spends more of a shared budget than it needs to.
async fn read_net(sink: &Arc<dyn crate::sink::OrderSink>, venue: &str) -> Result<NetMap, String> {
    let mut last = String::new();
    for attempt in 0..RETRIES {
        let s = sink.clone();
        match tokio::task::spawn_blocking(move || s.net_positions()).await {
            Ok(Ok(m)) => return Ok(m),
            Ok(Err(e)) => last = format!("{venue}: {e}"),
            Err(e) => last = format!("{venue}: read task failed: {e}"),
        }
        if attempt + 1 < RETRIES {
            tokio::time::sleep(RETRY_DELAY * (attempt + 1)).await;
        }
    }
    Err(last)
}

/// PM-US positions, believed only when two consecutive reads agree EXACTLY.
///
/// The port of `pmus_positions_consensus` (`hedge_naked_legs.py:74-86`),
/// including its comparison: the whole slug -> net map, not a subset and not a
/// tolerance. A row that appears, moves or vanishes between two reads two
/// seconds apart is the endpoint dropping rows, not the account trading.
async fn pmus_consensus(sink: &Arc<dyn crate::sink::OrderSink>) -> Result<NetMap, String> {
    let mut prev: Option<NetMap> = None;
    for attempt in 0..CONSENSUS_ATTEMPTS {
        let cur = read_net(sink, "pmus").await?;
        if prev.as_ref() == Some(&cur) {
            return Ok(cur);
        }
        prev = Some(cur);
        if attempt + 1 < CONSENSUS_ATTEMPTS {
            tokio::time::sleep(CONSENSUS_GAP).await;
        }
    }
    Err("pmus: positions unstable across reads — not acting".into())
}

// ------------------------------------------------------------------ acting ---

/// Everything the ARMED pass needs, and the state it keeps between cycles.
///
/// `None` on the `cycle` argument is the whole of `--positions-recon`'s
/// unchanged behaviour: no ledger read, no quote read, no order, and not one
/// extra branch on the detection path.
pub struct Act {
    /// DECIDE, LOG, AND STOP (`--positions-recon-act-shadow`).
    ///
    /// Everything above the wire runs — the venue reads, the ownership
    /// contract, the ledger basis, the limit, every refusal — and the order is
    /// printed instead of sent. It exists because the alternative is that the
    /// first time this code prices a real order is also the first time anyone
    /// has seen it price one: the basis reconstruction is the part that has
    /// never met live money, and the leg it was written for turns out to have
    /// NO ledger lot at all (see `naked_act`). A day of shadow output answers
    /// "what would it have done" without an account being involved.
    ///
    /// It still spends the venue read budget, which is why it is a flag and not
    /// the default.
    pub shadow: bool,
    /// The trade ledger — read for the cost basis, written for the fill.
    pub ledger_path: String,
    /// The research probes' own logs, re-read every cycle because they are
    /// live files. `arb_core::naked::ProbeOwnership` explains why this grep is
    /// doing 100% of the ownership work and the table labelled "authoritative"
    /// none of it.
    pub probe_logs: Vec<String>,
    cx: arb_core::scan::Cx,
    fees: arb_core::fees::FeeSchedule,
    /// Markets the VENUE has refused as halted, and until when.
    ///
    /// `Retry::MarketHalted` parking, on the same backoff the engine's hedge
    /// retry uses (`engine::hedge::venue_reopen_park`), so there is ONE halt
    /// policy in this binary rather than two. Be clear about what it buys at
    /// this cadence: the backoff tops out at 60s and this cycle is 300s apart,
    /// so a park has always expired by the next cycle and it is the quote's own
    /// `status` field that does the real work (`naked_act::decide` refuses
    /// anything not `active`). It is here because a halted venue must not be
    /// re-tried within a cycle either, and because a future shorter interval
    /// would otherwise reintroduce the 2026-07-30 storm.
    parked: BTreeMap<String, (std::time::Instant, u32)>,
}

impl Act {
    pub fn new(shadow: bool, ledger_path: String, probe_logs: Vec<String>) -> Act {
        let mut cx = arb_core::scan::Cx::default();
        let fees = arb_core::fees::FeeSchedule::new(&mut cx);
        Act { shadow, ledger_path, probe_logs, cx, fees, parked: BTreeMap::new() }
    }

    /// The probe-ownership set, rebuilt from the live logs.
    ///
    /// `Err` when NOT ONE of the sources could be read. `ProbeOwnership` is
    /// documented as FAIL-OPEN — "an unreadable DB, a missing log, a rolled-over
    /// window: each one silently converts a probe position into a hedger
    /// position" — and it ends by saying that a sweep wanting the other posture
    /// must check its sources loaded before it asks. This is a sweep that
    /// PLACES, so it checks. Some-but-not-all is still fail-open and still
    /// accepted, because refusing on one missing file would make an idle probe
    /// able to stop all hedging; zero-of-three is the case where the predicate
    /// carries no information at all.
    ///
    /// The `ownership` TABLE is not consulted. It is SQLite, this binary has no
    /// SQLite, and `arb_core::naked` establishes that it is empty and has never
    /// been written by anything — the source labelled authoritative contributes
    /// nothing and the transitional grep does all of it. Reading it would need a
    /// dependency to learn nothing.
    fn ownership(&self) -> Result<arb_core::naked::ProbeOwnership, String> {
        let mut own = arb_core::naked::ProbeOwnership::new();
        let mut read = 0usize;
        for p in &self.probe_logs {
            if let Ok(text) = std::fs::read_to_string(p) {
                own.absorb_probe_log(&text);
                read += 1;
            }
        }
        if read == 0 {
            return Err(format!(
                "none of the {} research-probe logs could be read, so the ownership \
                 predicate would answer `ours` to everything — refusing to act rather than \
                 complete a probe's open position",
                self.probe_logs.len()
            ));
        }
        Ok(own)
    }

    fn is_parked(&self, market: &str) -> bool {
        self.parked.get(market).is_some_and(|(until, _)| *until > std::time::Instant::now())
    }

    fn park(&mut self, market: &str) -> std::time::Duration {
        let strikes = self.parked.get(market).map(|(_, s)| *s).unwrap_or(0) + 1;
        let d = crate::engine::hedge::venue_reopen_park(strikes);
        self.parked.insert(market.to_string(), (std::time::Instant::now() + d, strikes));
        d
    }
}

/// Act on the findings THIS cycle confirmed.
///
/// Ordering is the caller's: `confirmed` comes out of `Recon::step`, whose
/// findings are in registry order, and nothing here re-sorts. The budget is
/// spent on PLACES, not on findings — a finding the guards decline costs
/// nothing, so a hundred refusals do not stop the one order that should go out.
async fn act(
    act: &mut Act,
    confirmed: &[Finding],
    kalshi: &Arc<dyn crate::sink::OrderSink>,
) -> Result<(), String> {
    if confirmed.is_empty() {
        return Ok(());
    }
    let own = act.ownership()?;
    // STRICT, unlike the duplicate check inside `append_basket`, and for the
    // opposite reason: this read decides whether to SPEND money, and a line it
    // cannot parse is exposure it cannot see. `ledger::read` is the same refusal
    // that gates arming.
    let records = crate::ledger::read(&act.ledger_path)
        .map_err(|e| format!("the ledger is unreadable ({e}) — nothing may be priced off it"))?;
    let now = now_s() as f64;
    let mut placed = 0usize;
    for f in confirmed {
        if placed >= crate::naked_act::MAX_ACTIONS_PER_CYCLE {
            eprintln!(
                "[recon-act] cycle budget spent ({} order(s)) — {} is still confirmed and \
                 will be reconsidered next cycle",
                crate::naked_act::MAX_ACTIONS_PER_CYCLE,
                f.rel_id
            );
            break;
        }
        // OWNERSHIP FIRST, before a quote is even fetched: a probe's market is
        // not ours to price, let alone to complete.
        //
        // Applied HERE rather than through `find`'s `excluded` seam on purpose.
        // The seam drops a pair silently, and `arb_core::naked` is explicit that
        // a probe-owned naked leg must be SURFACED and merely not completed —
        // "surface it, never complete it". Gating the act pass keeps detection
        // exactly as wide as it was (so `positions_recon_naked` still counts it,
        // and `--positions-recon` stays byte-identical) while making the
        // position untouchable.
        if !own.hedger_owns(&f.pmus) {
            ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon-act] NOT OURS — {}: a research probe is working {}, so this position \
                 is theirs to manage. Reported, never completed.",
                f.line(),
                f.pmus
            );
            continue;
        }
        if act.is_parked(&f.kalshi) {
            ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon-act] PARKED — {} is halted at the venue; not re-sending into it",
                f.kalshi
            );
            continue;
        }
        let k = kalshi.clone();
        let market = f.kalshi.clone();
        let quote = match tokio::task::spawn_blocking(move || k.market_quote(&market)).await {
            Ok(Ok(q)) => q,
            Ok(Err(e)) => {
                ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
                eprintln!("[recon-act] NO QUOTE for {} ({e}) — cannot price a hedge", f.kalshi);
                continue;
            }
            Err(e) => {
                ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
                eprintln!("[recon-act] quote task failed for {} ({e})", f.kalshi);
                continue;
            }
        };
        let order = match crate::naked_act::decide(
            &mut act.cx,
            &act.fees,
            &records,
            f,
            &quote,
            now,
        ) {
            Ok(o) => o,
            Err(why) => {
                ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
                eprintln!("[recon-act] NO — {}: {why}", f.rel_id);
                continue;
            }
        };
        placed += 1;
        place_and_book(act, f, &order, kalshi).await;
    }
    Ok(())
}

/// Send ONE order and book what it filled.
///
/// Split out because everything above it is a decision and everything below it
/// is money: the boundary is where the tests stop being able to help.
async fn place_and_book(
    st: &mut Act,
    f: &Finding,
    o: &crate::naked_act::Order,
    kalshi: &Arc<dyn crate::sink::OrderSink>,
) {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};
    // `n` + millis. A SEPARATE id space from the engine's `h`, so a double-hedge
    // post-mortem can tell whose order was whose — see `gateway::is_ours`, which
    // also has to recognise it or the kill sweep would not clean it up.
    let coid = format!(
        "n{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    let req = PlaceRequest {
        market: o.market.clone(),
        side: if o.buy { Side::Bid } else { Side::Ask },
        price: o.limit.clone(),
        qty: o.qty,
        tif: Tif::Ioc,
        // The half that actually reaches the wire as an IOC: both wire builders
        // inline the TIF from `post_only` and ignore `PlaceRequest::tif`
        // (`wire.rs`; `engine::hedge` says the same). Both are set so the two
        // cannot drift apart.
        post_only: false,
        client_order_id: coid.clone(),
    };
    eprintln!(
        "[recon-act]{} {} {}x {} IOC limit {} (ledger basis {}/ct from the open record at ts \
         {}, locks >= {}/ct) — id {coid}",
        if st.shadow { " SHADOW —" } else { "" },
        if o.buy { "BUY YES" } else { "SELL YES" },
        o.qty,
        o.market,
        o.limit,
        o.basis,
        o.lot_ts,
        crate::naked_act::MIN_LOCK
    );
    // THE LAST LINE BEFORE THE WIRE. Everything above ran; nothing below does.
    if st.shadow {
        return;
    }
    let k = kalshi.clone();
    let r = req.clone();
    let oid = match tokio::task::spawn_blocking(move || k.place(&r)).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) if e.retry() == arb_venue::error::Retry::MarketHalted => {
            ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
            let d = st.park(&o.market);
            eprintln!(
                "[recon-act] {} is HALTED at the venue ({e}) — parked for {}s. No price, size \
                 or interval answers a halt; only the venue reopening does.",
                o.market,
                d.as_secs()
            );
            return;
        }
        // The venue ANSWERED, and its answer was no. Nothing is at the venue and
        // nothing is owed — an ordinary refusal.
        Ok(Err(e @ arb_venue::VenueError::Status { .. })) => {
            ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
            eprintln!("[recon-act] PLACE REFUSED on {} ({e})", o.market);
            return;
        }
        // THE REQUEST NEVER COMPLETED, which is not the same as never happening.
        // A timeout is indistinguishable from an order that landed and whose
        // answer was lost — `exec::place_answer_was_lost` exists for exactly
        // this on the engine's path, and has a `recover_place` behind it. This
        // pass has no such recovery, so it says so on the must-stay-0 gauge
        // rather than filing it with the ordinary refusals.
        Ok(Err(e)) => {
            ACT_UNRESOLVED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon-act] PLACE DID NOT COMPLETE on {} ({e}) — the request never got an \
                 answer, so the order may or may not be at the venue and nothing here can \
                 tell. CHECK BY HAND: client_order_id {coid}",
                o.market
            );
            return;
        }
        Err(e) => {
            ACT_UNRESOLVED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon-act] PLACE TASK FAILED on {} ({e}) — same as above: the order may or \
                 may not be at the venue. CHECK BY HAND: client_order_id {coid}",
                o.market
            );
            return;
        }
    };
    // This order never went through `Engine::dispatch`, so no ack names it to
    // the engine and its fill would arrive on the account-wide feed as money
    // nothing there can explain. Tell it the venue's own id, which is the only
    // thing the fill frame will carry.
    crate::engine::fill::note_sidecar_order(&oid);
    // A place that ACCEPTED and a place that FILLED are different facts, and
    // only the second may be booked. `filled_qty` refuses rather than answering
    // 0 when it cannot ask (`OrderSink::filled_qty`), which is what keeps an
    // unreadable venue from being booked as an unfilled order.
    let mut filled = 0i64;
    let mut unreadable: Option<String> = None;
    for i in 0..crate::naked_act::FILL_POLLS {
        let k = kalshi.clone();
        let id = oid.clone();
        match tokio::task::spawn_blocking(move || k.filled_qty(&id)).await {
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
        // NOT "it did not fill". We sent a real order and cannot say what
        // happened to it, so the position may be real and unbooked — which is
        // the one state nothing downstream can see.
        eprintln!(
            "[recon-act] UNREADABLE FILL on {} ({e}) — order {oid} (client {coid}) was ACCEPTED \
             by the venue and this process cannot tell whether it traded. It is NOT booked. \
             RECONCILE BY HAND before the next cycle, which will otherwise see the same \
             imbalance and consider it again. positions_recon_act_unresolved is now {}.",
            o.market,
            ACT_UNRESOLVED.fetch_add(1, Ordering::Relaxed) + 1
        );
        return;
    }
    if filled < 1 {
        eprintln!(
            "[recon-act] {} IOC unfilled — the book moved; next cycle reconsiders it",
            o.market
        );
        ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    // A venue that reports MORE filled than we ordered. The same clamp
    // `hedge_credit` applies to a maker frame, and the same reason it is not
    // silent there: the excess contracts are real and there is no maker leg to
    // pair them with, so booking them would invent a basket that never existed
    // — and swallowing them would hide a position from every exposure fold.
    if filled > o.qty {
        eprintln!(
            "[recon-act] OVER-FILL on {}: the venue reports {filled} filled against an order \
             for {}. {} contract(s) are ours with nothing booked against them — this is \
             directional exposure that no cap and no unwind can see. RECONCILE BY HAND.",
            o.market,
            o.qty,
            filled - o.qty
        );
        ACT_UNRESOLVED.fetch_add(1, Ordering::Relaxed);
    }
    let filled = filled.min(o.qty);
    // WHAT KALSHI SAYS IT TRADED AT, not what we told it to accept. A marketable
    // limit is a ceiling on a buy and a floor on a sell, never a prediction, and
    // this path booked the limit until now — the log line said `at <= 0.1000`
    // and the ledger recorded 0.1000. That understated a completion's edge, and
    // understating it is not harmless here: `naked_act::worst_lot` folds these
    // very records into the basis the NEXT completion has to beat, so a dear
    // basis makes future profitable orders look unprofitable and refuse.
    //
    // `o.buy` decides the direction, NOT the venue: this is the one caller that
    // goes both ways on Kalshi — Case A buys the missing YES, Case B sells the
    // excess one — and `fill_price`'s "never worse than its limit" guard inverts
    // with it. Falls back to `o.limit` when the venue will not answer, which is
    // exactly this path's behaviour before the read existed.
    let k_px = crate::maker_exit::filled_price_or_limit(
        &mut st.cx,
        kalshi,
        &oid,
        &o.limit,
        !o.buy,
    )
    .await;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let rec = if o.buy {
        crate::naked_act::basket_record(f, o, &k_px, filled, ts)
    } else {
        crate::naked_act::close_record(f, o, &k_px, filled, ts)
    };
    // Through `append_basket`, never a raw append: the single-author rule is
    // what caught the 2026-07-30 double-book, and this is precisely the writer
    // it was written about.
    match crate::ledger::append_basket(&st.ledger_path, rec) {
        Ok(crate::ledger::Booking::Booked) => {
            ACTED.fetch_add(1, Ordering::Relaxed);
            // A price EQUAL to the limit is two different facts — the venue
            // said so, or the venue would not say and we fell back — and
            // `filled_price_or_limit` folds them into one string. So the
            // hedged phrasing survives for that case: it is still everything
            // this line can honestly claim.
            if k_px == o.limit {
                eprintln!(
                    "[recon-act] FILLED {}x {} at <= {} — booked",
                    filled, o.market, o.limit
                );
            } else {
                eprintln!(
                    "[recon-act] FILLED {}x {} @ {} (venue truth; limit was {}) — booked",
                    filled, o.market, k_px, o.limit
                );
            }
        }
        Ok(crate::ledger::Booking::AlreadyBooked) => {
            ACTED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon-act] FILLED {}x {} but a record with this exact (relationship, ts) is \
                 ALREADY in {} — not written twice. CHECK BY HAND: this should be \
                 unreachable, and if it is not then two writers share a clock.",
                filled, o.market, st.ledger_path
            );
        }
        Ok(crate::ledger::Booking::Contested(others)) => {
            ACTED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon-act] CONTESTED {} — another writer already booked an OPEN basket on \
                 {} + {} at ts {others:?}, and this pass has just booked its own fill. BOTH \
                 are in the ledger and BOTH count as exposure. THIS IS THE DOUBLE HEDGE \
                 SIGNATURE: if arbbot-hedge.timer is still armed, STOP IT, then reconcile \
                 the account on {} by hand.",
                f.rel_id, f.pmus, o.market, o.market
            );
        }
        Err(e) => eprintln!(
            "[recon-act] LEDGER WRITE FAILED ({e}) — {}x {} FILLED AND IS NOT BOOKED. This \
             position is invisible to every exposure fold, so the caps will free themselves \
             against it and nothing will unwind it. FIX BY HAND.",
            filled, o.market
        ),
    }
}

/// One reconciliation cycle. `Err` = the cycle was abandoned; NOTHING is
/// concluded from it, and in particular no gauge moves except the failure
/// counter.
async fn cycle(
    recon: &mut Recon,
    kalshi: &Arc<dyn crate::sink::OrderSink>,
    pmus: &Arc<dyn crate::sink::OrderSink>,
    acting: Option<&mut Act>,
) -> Result<(), String> {
    // PM-US first: it is the read with the documented glitches, so a cycle that
    // is going to be abandoned is abandoned before spending a Kalshi token.
    let ppos = pmus_consensus(pmus).await?;
    // Kalshi gets `read_net`'s retry but no consensus, which is what the Python
    // does — its list has no history of dropping rows. A transient bad Kalshi
    // read is caught one layer up instead: guard 3 will not confirm it.
    let kpos = read_net(kalshi, "kalshi").await?;
    let (confirmed, fresh) = recon.step(&kpos, &ppos, &|_| false);
    // Published only HERE, past every guard: the gauges are the reading of a
    // snapshot this cycle was willing to believe.
    NAKED.store(confirmed.len() as i64, Ordering::Relaxed);
    UNCONFIRMED.store(fresh.len() as i64, Ordering::Relaxed);
    LAST_OK_S.store(now_s(), Ordering::Relaxed);
    for f in &confirmed {
        eprintln!("[recon] {}", f.line());
    }
    for f in &fresh {
        eprintln!("[recon] unconfirmed (awaiting a second cycle) — {}", f.line());
    }
    eprintln!(
        "[recon] ok — {} pair(s) reconciled against {} kalshi / {} pmus venue positions; \
         positions_recon_naked={} unconfirmed={}",
        recon.pairs().len(),
        kpos.len(),
        ppos.len(),
        confirmed.len(),
        fresh.len()
    );
    // ACTING IS THE LAST THING AND CANNOT FAIL THE CYCLE. The read has already
    // been published above, so a refusal here leaves the reconciliation itself
    // intact and reported — the detector must not go dark because the actor
    // could not do its job.
    if let Some(a) = acting {
        if let Err(e) = act(a, &confirmed, kalshi).await {
            ACT_REFUSED.fetch_add(1, Ordering::Relaxed);
            eprintln!("[recon-act] NOTHING ACTED ON THIS CYCLE: {e}");
        }
    }
    Ok(())
}

/// The periodic loop. Runs until the process exits.
pub async fn recon_loop(
    mut recon: Recon,
    kalshi: Arc<dyn crate::sink::OrderSink>,
    pmus: Arc<dyn crate::sink::OrderSink>,
    mut acting: Option<Act>,
) {
    // `tokio::time::interval`'s first tick is ready immediately, and that is
    // wanted: the first cycle takes the baseline snapshot that the second can
    // confirm against, so an orphan this process inherited is reported five
    // minutes in rather than ten.
    let mut iv = tokio::time::interval(INTERVAL);
    loop {
        iv.tick().await;
        if let Err(e) = cycle(&mut recon, &kalshi, &pmus, acting.as_mut()).await {
            FAILURES.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[recon] CYCLE ABANDONED ({e}) — no conclusion drawn; positions_recon_naked={} \
                 is now {}s old (positions_recon_failures={})",
                naked(),
                age_s(),
                failures()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_venue::gateway::{CancelRequest, PlaceRequest};
    use arb_venue::VenueError;
    use std::sync::Mutex;

    fn pair(id: &str) -> Pair {
        Pair { rel_id: id.into(), kalshi: format!("K-{id}"), pmus: format!("p-{id}") }
    }

    fn net(v: &[(&str, f64)]) -> NetMap {
        v.iter().map(|(k, q)| ((*k).to_string(), *q)).collect()
    }

    fn nobody(_: &Pair) -> bool {
        false
    }

    /// A hedged basket is Kalshi-long against PM-short and nets to zero.
    #[test]
    fn a_hedged_basket_is_not_naked() {
        let f = find(&[pair("a")], &net(&[("K-a", 25.0)]), &net(&[("p-a", -25.0)]), &nobody);
        assert!(f.is_empty(), "{f:?}");
    }

    /// The case the Python acts on: PM sold more than Kalshi covers.
    #[test]
    fn a_pm_short_over_the_kalshi_long_is_naked() {
        let f = find(&[pair("a")], &net(&[("K-a", 20.0)]), &net(&[("p-a", -25.0)]), &nobody);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].leg, Leg::PmShort);
        assert_eq!(f[0].qty, 5, "the contracts a completing hedge would buy");
        assert_eq!(f[0].imb, -5.0);
    }

    /// ...and the other direction is a DIFFERENT finding, not the same one
    /// with a sign. The Python alerts it and refuses to auto-hedge it; the
    /// policy is a sibling change's, and it needs the two told apart to have
    /// anywhere to hang.
    #[test]
    fn a_kalshi_long_over_the_pm_short_is_its_own_case() {
        let f = find(&[pair("a")], &net(&[("K-a", 30.0)]), &net(&[("p-a", -25.0)]), &nobody);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].leg, Leg::KalshiLong);
        assert_eq!(f[0].qty, 5);
    }

    /// The deadband, mirrored from `hedge_naked_legs.py:174-177` INCLUDING its
    /// asymmetry. Fractional dust — Kalshi's `position_fp` really is fractional
    /// — must not read as an exposed contract.
    #[test]
    fn the_deadband_is_asymmetric_and_swallows_dust() {
        for (kq, pq) in [(25.0, -25.4), (25.0, -25.5), (25.4, -25.0), (25.9, -25.0)] {
            let f = find(&[pair("a")], &net(&[("K-a", kq)]), &net(&[("p-a", pq)]), &nobody);
            assert!(f.is_empty(), "imb {:+} must be inside the deadband: {f:?}", kq + pq);
        }
        // just outside, on each side
        assert_eq!(
            find(&[pair("a")], &net(&[("K-a", 25.0)]), &net(&[("p-a", -25.6)]), &nobody).len(),
            1
        );
        assert_eq!(
            find(&[pair("a")], &net(&[("K-a", 26.0)]), &net(&[("p-a", -25.0)]), &nobody).len(),
            1
        );
    }

    /// A PM slug the read did not return is SKIPPED, never read as zero. This
    /// is the difference between a dropped row being a missed detection and a
    /// dropped row being a false naked that buys contracts.
    #[test]
    fn a_pm_slug_absent_from_the_read_is_skipped_not_zero() {
        let f = find(&[pair("a")], &net(&[("K-a", 25.0)]), &net(&[("p-other", -1.0)]), &nobody);
        assert!(f.is_empty(), "a missing PM row must not make our Kalshi long look naked: {f:?}");
    }

    /// ...whereas a missing KALSHI ticker IS zero, because that is what the
    /// venue means by omitting it, and a PM short with no Kalshi cover is the
    /// exact thing being looked for. It is also the read that a transient
    /// Kalshi failure would fake, which is why nothing acts on one cycle.
    #[test]
    fn a_missing_kalshi_ticker_is_a_real_zero() {
        let f = find(&[pair("a")], &net(&[]), &net(&[("p-a", -3.0)]), &nobody);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].leg, Leg::PmShort);
        assert_eq!(f[0].qty, 3);
    }

    /// The ownership seam: an excluded pair produces no finding at all.
    #[test]
    fn an_excluded_pair_is_not_ours_to_call_naked() {
        let owned = |p: &Pair| p.pmus == "p-a";
        let f = find(
            &[pair("a"), pair("b")],
            &net(&[]),
            &net(&[("p-a", -9.0), ("p-b", -9.0)]),
            &owned,
        );
        assert_eq!(f.len(), 1, "only the unowned pair: {f:?}");
        assert_eq!(f[0].rel_id, "b");
    }

    /// Two registry entries over the same market pair are ONE position.
    #[test]
    fn duplicate_pairs_are_reported_once() {
        let mut dup = pair("a");
        dup.rel_id = "a-again".into();
        let f = find(&[pair("a"), dup], &net(&[]), &net(&[("p-a", -4.0)]), &nobody);
        assert_eq!(f.len(), 1, "{f:?}");
    }

    /// GUARD 3. One sighting is never enough: the sticky-stale PM cache
    /// survives back-to-back reads, so only a second CYCLE can tell a real
    /// naked leg from a cached one.
    #[test]
    fn an_imbalance_is_reported_only_after_a_second_cycle_agrees() {
        let mut r = Recon::new(vec![pair("a")]);
        let (k, p) = (net(&[("K-a", 20.0)]), net(&[("p-a", -25.0)]));
        let (confirmed, fresh) = r.step(&k, &p, &nobody);
        assert!(confirmed.is_empty(), "first sighting must not be reported");
        assert_eq!(fresh.len(), 1);
        let (confirmed, fresh) = r.step(&k, &p, &nobody);
        assert_eq!(confirmed.len(), 1, "the second agreeing cycle reports it");
        assert!(fresh.is_empty());
    }

    /// ...and an imbalance that CHANGES between cycles is two observations,
    /// not one confirmation. A leg being hedged by the 5-minute timer while
    /// this watches looks exactly like this, and it must not alarm.
    #[test]
    fn a_moving_imbalance_never_confirms_itself() {
        let mut r = Recon::new(vec![pair("a")]);
        r.step(&net(&[("K-a", 20.0)]), &net(&[("p-a", -25.0)]), &nobody);
        let (confirmed, fresh) = r.step(&net(&[("K-a", 22.0)]), &net(&[("p-a", -25.0)]), &nobody);
        assert!(confirmed.is_empty(), "a different size is a different sighting: {confirmed:?}");
        assert_eq!(fresh.len(), 1);
    }

    /// A leg that goes away between cycles is forgotten, and cannot be
    /// confirmed later by a sighting that agrees with a stale memory. This is
    /// the 2026-07-30 shape: the engine reported `hedges_naked: 1` for a leg
    /// the timer had already closed.
    #[test]
    fn a_leg_the_other_owner_closed_stops_being_reported() {
        let mut r = Recon::new(vec![pair("a")]);
        let (k, p) = (net(&[("K-a", 20.0)]), net(&[("p-a", -25.0)]));
        r.step(&k, &p, &nobody);
        let (confirmed, _) = r.step(&k, &p, &nobody);
        assert_eq!(confirmed.len(), 1);
        // the timer completes the basket
        let (confirmed, fresh) = r.step(&net(&[("K-a", 25.0)]), &p, &nobody);
        assert!(
            confirmed.is_empty() && fresh.is_empty(),
            "venue truth retracts it without anything telling us: {confirmed:?} {fresh:?}"
        );
    }

    // ---- the IO guards, against a sink that fails the way the venues do ----

    /// The gauges above are process-wide, and `cargo test` runs these
    /// concurrently in one process. Every test that asserts on one takes this
    /// first. Async, because the workspace denies `await_holding_lock` and
    /// these hold it across the cycle they are measuring.
    pub(super) static GAUGES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Answers a canned sequence of `net_positions` results, one per call.
    struct Reads {
        replies: Mutex<Vec<Result<NetMap, VenueError>>>,
        calls: AtomicU64,
    }

    impl Reads {
        fn new(replies: Vec<Result<NetMap, VenueError>>) -> Arc<Self> {
            Arc::new(Reads { replies: Mutex::new(replies), calls: AtomicU64::new(0) })
        }
    }

    impl crate::sink::OrderSink for Reads {
        fn place(&self, _: &PlaceRequest) -> Result<String, VenueError> {
            panic!("the reconciliation must never place")
        }
        fn cancel(&self, _: &CancelRequest) -> Result<(), VenueError> {
            panic!("the reconciliation must never cancel")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            panic!("the reconciliation must never sweep")
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            Ok(Vec::new())
        }
        fn net_positions(&self) -> Result<NetMap, VenueError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut r = self.replies.lock().expect("replies");
            if r.is_empty() {
                panic!("unexpected extra positions read");
            }
            r.remove(0)
        }
    }

    fn boom() -> Result<NetMap, VenueError> {
        Err(VenueError::Status {
            endpoint: "pmus positions",
            status: 503,
            body: "service unavailable".into(),
        })
    }

    /// THE 2026-07-31 SHAPE. PM-US answered 503 over and over. Whatever else
    /// happens, the cycle must not conclude that a book with no readable PM
    /// positions has nothing naked in it.
    #[tokio::test(start_paused = true)]
    async fn a_venue_that_will_not_answer_abandons_the_cycle() {
        let _g = GAUGES.lock().await;
        let pmus = Reads::new(vec![boom(), boom(), boom()]);
        let kalshi = Reads::new(vec![]);
        let mut r = Recon::new(vec![pair("a")]);
        let before = (naked(), unconfirmed(), LAST_OK_S.load(Ordering::Relaxed));
        let (kd, pd) = (
            kalshi.clone() as Arc<dyn crate::sink::OrderSink>,
            pmus.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let e = cycle(&mut r, &kd, &pd, None).await.expect_err("a 503 is not an empty portfolio");
        assert!(e.contains("503"), "the venue's own answer survives to the log: {e}");
        assert_eq!(pmus.calls.load(Ordering::Relaxed), RETRIES as u64, "retried, then refused");
        assert_eq!(kalshi.calls.load(Ordering::Relaxed), 0, "and never spent a Kalshi token");
        assert_eq!(
            (naked(), unconfirmed(), LAST_OK_S.load(Ordering::Relaxed)),
            before,
            "no gauge may move on a failed read — least of all the freshness stamp"
        );
    }

    /// A refusal must not be able to CLEAR a standing alarm either: an
    /// unreadable venue is not evidence that the leg was hedged.
    #[tokio::test(start_paused = true)]
    async fn a_failed_cycle_does_not_retract_a_confirmed_leg() {
        let _g = GAUGES.lock().await;
        let pm = || Ok(net(&[("p-a", -25.0)]));
        let ka = || Ok(net(&[("K-a", 20.0)]));
        let good_pm = Reads::new(vec![pm(), pm(), pm(), pm()]);
        let good_k = Reads::new(vec![ka(), ka()]);
        let (kd, pd) = (
            good_k as Arc<dyn crate::sink::OrderSink>,
            good_pm as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a")]);
        cycle(&mut r, &kd, &pd, None).await.expect("clean");
        cycle(&mut r, &kd, &pd, None).await.expect("clean");
        assert_eq!(naked(), 1);

        let dead = Reads::new(vec![boom(), boom(), boom()]) as Arc<dyn crate::sink::OrderSink>;
        let idle = Reads::new(vec![]) as Arc<dyn crate::sink::OrderSink>;
        assert!(cycle(&mut r, &idle, &dead, None).await.is_err());
        assert_eq!(naked(), 1, "the last TRUSTED reading stands");
        // ...and the next good cycle still confirms against it, because a
        // refused cycle neither confirmed nor forgot.
        let (confirmed, _) = r.step(&net(&[("K-a", 20.0)]), &net(&[("p-a", -25.0)]), &nobody);
        assert_eq!(confirmed.len(), 1);
    }

    /// GUARD 2. Two reads that disagree are the endpoint dropping rows. The
    /// run refuses rather than picking one — including when the disagreement
    /// is only ever between consecutive pairs.
    #[tokio::test(start_paused = true)]
    async fn positions_that_will_not_settle_are_refused() {
        let pmus = Reads::new(vec![
            Ok(net(&[("p-a", -25.0)])),
            Ok(net(&[("p-a", -25.0), ("p-b", -5.0)])),
            Ok(net(&[("p-a", -25.0)])),
            Ok(net(&[("p-a", -25.0), ("p-b", -5.0)])),
        ]);
        let e = pmus_consensus(&(pmus.clone() as Arc<dyn crate::sink::OrderSink>))
            .await
            .expect_err("no two consecutive reads agreed");
        assert!(e.contains("unstable"), "{e}");
        assert_eq!(pmus.calls.load(Ordering::Relaxed), CONSENSUS_ATTEMPTS as u64);
    }

    /// ...and two that DO agree are believed, even if it took a glitched read
    /// to get there. This is the ordinary path: one empty-glitch refusal,
    /// then agreement.
    #[tokio::test(start_paused = true)]
    async fn two_agreeing_reads_are_believed() {
        let pmus = Reads::new(vec![
            Err(VenueError::Status {
                endpoint: "pmus positions",
                status: 0,
                body: "EMPTY positions map".into(),
            }),
            Ok(net(&[("p-a", -25.0)])),
            Ok(net(&[("p-a", -25.0)])),
        ]);
        let m = pmus_consensus(&(pmus as Arc<dyn crate::sink::OrderSink>)).await.expect("agreed");
        assert_eq!(m, net(&[("p-a", -25.0)]));
    }

    /// The whole path, on sinks: two clean cycles turn a real imbalance into a
    /// confirmed one, and the reads are consensus'd every time.
    #[tokio::test(start_paused = true)]
    async fn two_clean_cycles_confirm_a_real_naked_leg() {
        let _g = GAUGES.lock().await;
        let pm = || Ok(net(&[("p-a", -25.0)]));
        let ka = || Ok(net(&[("K-a", 20.0)]));
        let pmus = Reads::new(vec![pm(), pm(), pm(), pm()]);
        let kalshi = Reads::new(vec![ka(), ka()]);
        let (kd, pd) = (
            kalshi.clone() as Arc<dyn crate::sink::OrderSink>,
            pmus.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a")]);
        cycle(&mut r, &kd, &pd, None).await.expect("clean");
        assert_eq!(naked(), 0, "one sighting is not a report");
        cycle(&mut r, &kd, &pd, None).await.expect("clean");
        assert_eq!(naked(), 1);
        // Wall-clock, not the paused runtime clock, so assert the property
        // (a completed cycle is fresh) rather than an exact second.
        assert!((0..5).contains(&age_s()), "a completed cycle is fresh: {}", age_s());
    }
}

/// The ARMED pass, against a venue double that answers the way the real one
/// does — including the ways it refuses.
///
/// These are the tests that stand between a confirmed finding and a real order,
/// so each one names the thing that goes wrong without it rather than the branch
/// it covers.
#[cfg(test)]
mod act_tests {
    use super::tests::GAUGES;
    use super::*;
    use arb_venue::gateway::{CancelRequest, PlaceRequest, Quote};
    use arb_venue::VenueError;
    use std::sync::Mutex;

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join(format!("arb-trader-reconact-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("scratch");
        d
    }

    /// A ledger holding ONE open basket: PM-US short YES at 0.22 (0.78 a
    /// contract of NO) against 5 Kalshi YES at 0.19.
    fn ledger_with_one_open_basket(dir: &std::path::Path) -> String {
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            concat!(
                r#"{"ts":1.0,"relationship_id":"a","status":"open","qty":5,"legs":[{"venue":"#,
                r#""polymarket_us","market_id":"p-a","side":"no","role":"maker","qty":5,"#,
                r#""yes_price":"0.22"},{"venue":"kalshi","market_id":"K-a","side":"yes","#,
                r#""role":"taker","qty":5,"yes_price":"0.19"}]}"#,
                "\n"
            ),
        )
        .expect("ledger");
        p.to_string_lossy().into_owned()
    }

    /// One readable probe log claiming `slug`. `ProbeOwnership` is fail-open on
    /// an unreadable source, so the act pass refuses when it can read NONE —
    /// every test therefore has to give it at least one.
    fn probe_log(dir: &std::path::Path, slug: &str) -> Vec<String> {
        let p = dir.join("probe.jsonl");
        std::fs::write(&p, format!("{{\"pm\":\"{slug}\",\"action\":\"fill\",\"qty\":5}}\n"))
            .expect("probe log");
        vec![p.to_string_lossy().into_owned()]
    }

    fn quote(status: &str, bid: &str, ask: &str) -> Quote {
        Quote {
            market: "K-a".into(),
            status: status.into(),
            yes_bid: Some(bid.into()),
            yes_ask: Some(ask.into()),
            ladder: vec![("0.0000".into(), "1.0000".into(), "0.0100".into())],
        }
    }

    /// A Kalshi that quotes, places and reports fills — scripted, and recording
    /// every order it was sent.
    struct FakeKalshi {
        quote: Mutex<Result<Quote, VenueError>>,
        place: Mutex<Result<String, VenueError>>,
        filled: Mutex<Result<i64, VenueError>>,
        positions: Mutex<Vec<Result<NetMap, VenueError>>>,
        pub placed: Mutex<Vec<PlaceRequest>>,
        pub quotes_asked: AtomicU64,
        /// A line another writer appends to the ledger WHILE our place is on the
        /// wire — the 600ms race of 2026-07-30, which the pre-flight ledger read
        /// cannot see because it happened after that read.
        pub write_on_place: Mutex<Option<(String, String)>>,
    }

    impl FakeKalshi {
        fn new(kpos: Vec<Result<NetMap, VenueError>>) -> Arc<Self> {
            Arc::new(FakeKalshi {
                quote: Mutex::new(Ok(quote("active", "0.18", "0.19"))),
                place: Mutex::new(Ok("srv-1".into())),
                filled: Mutex::new(Ok(3)),
                positions: Mutex::new(kpos),
                placed: Mutex::new(Vec::new()),
                quotes_asked: AtomicU64::new(0),
                write_on_place: Mutex::new(None),
            })
        }
        fn placed(&self) -> Vec<PlaceRequest> {
            self.placed.lock().expect("placed").clone()
        }
    }

    impl crate::sink::OrderSink for FakeKalshi {
        fn place(&self, r: &PlaceRequest) -> Result<String, VenueError> {
            self.placed.lock().expect("placed").push(r.clone());
            if let Some((path, line)) = self.write_on_place.lock().expect("race").take() {
                use std::io::Write as _;
                let mut f = std::fs::OpenOptions::new().append(true).open(path).expect("race");
                writeln!(f, "{line}").expect("race");
            }
            self.place.lock().expect("place").clone()
        }
        fn cancel(&self, _: &CancelRequest) -> Result<(), VenueError> {
            panic!("the act pass sends IOCs; it never cancels")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            panic!("the act pass never sweeps")
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            Ok(Vec::new())
        }
        fn filled_qty(&self, _: &str) -> Result<i64, VenueError> {
            self.filled.lock().expect("filled").clone()
        }
        fn net_positions(&self) -> Result<NetMap, VenueError> {
            let mut p = self.positions.lock().expect("positions");
            if p.is_empty() {
                panic!("unexpected extra kalshi positions read");
            }
            p.remove(0)
        }
        fn market_quote(&self, _: &str) -> Result<Quote, VenueError> {
            self.quotes_asked.fetch_add(1, Ordering::Relaxed);
            self.quote.lock().expect("quote").clone()
        }
    }

    /// A PM-US that only ever answers positions, the way the real read does.
    struct FakePmus(Mutex<Vec<Result<NetMap, VenueError>>>);

    impl crate::sink::OrderSink for FakePmus {
        fn place(&self, _: &PlaceRequest) -> Result<String, VenueError> {
            panic!("this stack has no PM-US order path for naked legs")
        }
        fn cancel(&self, _: &CancelRequest) -> Result<(), VenueError> {
            panic!("no")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            panic!("no")
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            Ok(Vec::new())
        }
        fn net_positions(&self) -> Result<NetMap, VenueError> {
            let mut p = self.0.lock().expect("positions");
            if p.is_empty() {
                panic!("unexpected extra pmus positions read");
            }
            p.remove(0)
        }
    }

    fn net(v: &[(&str, f64)]) -> NetMap {
        v.iter().map(|(k, q)| ((*k).to_string(), *q)).collect()
    }

    fn pair(id: &str) -> Pair {
        Pair { rel_id: id.into(), kalshi: format!("K-{id}"), pmus: format!("p-{id}") }
    }

    /// PM short 5, Kalshi long 2 — a naked leg of 3, held steady so that two
    /// cycles CONFIRM it. Enough reads for `n` cycles of the consensus (2 PM
    /// reads a cycle after the first) plus the Kalshi position read.
    fn steady(cycles: usize) -> (Arc<FakeKalshi>, Arc<FakePmus>) {
        let pm: Vec<_> = (0..cycles * 2).map(|_| Ok(net(&[("p-a", -5.0)]))).collect();
        let k: Vec<_> = (0..cycles).map(|_| Ok(net(&[("K-a", 2.0)]))).collect();
        (FakeKalshi::new(k), Arc::new(FakePmus(Mutex::new(pm))))
    }

    /// Two cycles, so guard 3 confirms. Returns the act state so a test can
    /// inspect it afterwards.
    async fn two_cycles(st: &mut Act, k: &Arc<FakeKalshi>, p: &Arc<FakePmus>) {
        let (kd, pd) = (
            k.clone() as Arc<dyn crate::sink::OrderSink>,
            p.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a")]);
        cycle(&mut r, &kd, &pd, Some(st)).await.expect("cycle 1");
        cycle(&mut r, &kd, &pd, Some(st)).await.expect("cycle 2");
    }

    /// THE WHOLE PATH. A naked leg the venue confirms twice becomes one real
    /// IOC at a price the LEDGER justified, and a booked basket.
    #[tokio::test(start_paused = true)]
    async fn a_confirmed_pm_short_is_completed_at_a_ledger_priced_limit_and_booked() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("happy");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "some-other-probe-slug"));
        let (k, p) = steady(2);
        two_cycles(&mut st, &k, &p).await;

        let sent = k.placed();
        assert_eq!(sent.len(), 1, "ONE order, and only after the second cycle agreed");
        assert_eq!(sent[0].market, "K-a");
        assert_eq!(sent[0].side, arb_venue::gateway::Side::Bid, "buying the missing YES");
        assert_eq!(sent[0].qty, 3, "the hole, not the whole leg");
        assert!(!sent[0].post_only, "a hedge that rests is not a hedge");
        assert_eq!(sent[0].tif, arb_venue::gateway::Tif::Ioc);
        assert!(
            sent[0].client_order_id.starts_with('n'),
            "its own id space, so a double-hedge post-mortem can tell it from the engine's \
             `h`: {}",
            sent[0].client_order_id
        );
        // The limit is the LEDGER's, not the book's: 0.78 of basis leaves at
        // most ~0.215, and it must be at or above the 0.19 ask we lifted.
        let limit: f64 = sent[0].price.parse().expect("a price");
        assert!((0.19..=0.215).contains(&limit), "priced off the ledger basis: {limit}");

        let booked = crate::ledger::read(&ledger).expect("readable");
        let ours: Vec<_> = booked
            .iter()
            .filter(|r| r.get("source").and_then(|v| v.as_str()) == Some(crate::ledger::SOURCE))
            .collect();
        assert_eq!(ours.len(), 1, "the fill is booked exactly once: {booked:?}");
        assert_eq!(ours[0]["qty"], 3);
        assert_eq!(ours[0]["status"], "open");
    }

    /// THE BLAST-RADIUS CAP. A venue snapshot that is wrong about the whole book
    /// is wrong about every pair for the same reason, so three confirmed
    /// findings must cost two orders and not three. Guard 3 makes a wrong
    /// snapshot have to repeat itself; this bounds what happens when it does.
    #[tokio::test(start_paused = true)]
    async fn one_cycle_never_sends_more_orders_than_its_budget() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("budget");
        let p_ledger = d.join("trades.jsonl");
        let mut text = String::new();
        for id in ["a", "b", "c"] {
            text.push_str(&format!(
                concat!(
                    r#"{{"ts":1.0,"relationship_id":"{0}","status":"open","qty":5,"legs":[{{"#,
                    r#""venue":"polymarket_us","market_id":"p-{0}","side":"no","role":"maker","#,
                    r#""qty":5,"yes_price":"0.22"}},{{"venue":"kalshi","market_id":"K-{0}","#,
                    r#""side":"yes","role":"taker","qty":5,"yes_price":"0.19"}}]}}"#,
                    "\n"
                ),
                id
            ));
        }
        std::fs::write(&p_ledger, text).expect("ledger");
        let mut st =
            Act::new(false, p_ledger.to_string_lossy().into_owned(), probe_log(&d, "other"));

        let pm = || Ok(net(&[("p-a", -5.0), ("p-b", -5.0), ("p-c", -5.0)]));
        let ka = || Ok(net(&[("K-a", 2.0), ("K-b", 2.0), ("K-c", 2.0)]));
        let k = FakeKalshi::new(vec![ka(), ka()]);
        let p = Arc::new(FakePmus(Mutex::new(vec![pm(), pm(), pm(), pm()])));
        let (kd, pd) = (
            k.clone() as Arc<dyn crate::sink::OrderSink>,
            p.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a"), pair("b"), pair("c")]);
        cycle(&mut r, &kd, &pd, Some(&mut st)).await.expect("cycle 1");
        cycle(&mut r, &kd, &pd, Some(&mut st)).await.expect("cycle 2");
        assert_eq!(naked(), 3, "all three are confirmed and reported");
        assert_eq!(
            k.placed().len(),
            crate::naked_act::MAX_ACTIONS_PER_CYCLE,
            "and only the budget is spent: {:?}",
            k.placed().iter().map(|r| r.market.clone()).collect::<Vec<_>>()
        );
    }

    /// CASE B, END TO END — the case the frozen Python refuses outright ("not
    /// auto-hedged (v1)"), which is why two of these have stood for more than 30
    /// hours. A Kalshi long in excess of the PM short it was booked against is
    /// SOLD, above its own ledger basis net of the taker fee, and the record it
    /// writes CLOSES the lot it beat rather than opening a new basket.
    #[tokio::test(start_paused = true)]
    async fn a_confirmed_excess_kalshi_long_is_sold_above_its_basis_and_closes_its_lot() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("caseb");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "other"));

        // Kalshi long 5 against a PM short of only 2: three contracts in excess.
        let pm = || Ok(net(&[("p-a", -2.0)]));
        let ka = || Ok(net(&[("K-a", 5.0)]));
        let k = FakeKalshi::new(vec![ka(), ka()]);
        let p = Arc::new(FakePmus(Mutex::new(vec![pm(), pm(), pm(), pm()])));
        *k.quote.lock().expect("quote") = Ok(quote("active", "0.40", "0.42"));
        two_cycles(&mut st, &k, &p).await;

        let sent = k.placed();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].side,
            arb_venue::gateway::Side::Ask,
            "SELLING the excess — a buy here would double the position it is closing"
        );
        assert_eq!(sent[0].qty, 3, "the excess, not the whole long");
        // The limit is the profit FLOOR, not the touch: it must clear the ~0.20
        // ledger basis net of fees, and it must not be the 0.40 bid, because a
        // limit at the bid can trade below it when the book moves.
        let limit: f64 = sent[0].price.parse().expect("a price");
        assert!((0.21..0.40).contains(&limit), "a floor above the basis, not the touch: {limit}");

        let booked = crate::ledger::read(&ledger).expect("readable");
        let ours: Vec<_> = booked
            .iter()
            .filter(|r| r.get("source").and_then(|v| v.as_str()) == Some(crate::ledger::SOURCE))
            .collect();
        assert_eq!(ours.len(), 1);
        assert_eq!(ours[0]["status"], "unwound", "this REDUCES a position, it does not open one");
        assert_eq!(ours[0]["closes_ts"], 1.0, "and it names the lot whose basis it beat");
        assert_eq!(ours[0]["qty"], 3);
        // The fold the risk caps read must come down, not up.
        assert_eq!(
            crate::ledger::open_exposure(booked).get("a").copied(),
            Some(2.0),
            "5 open, 3 closed"
        );
    }

    /// ...and it HOLDS when the book will not pay, rather than dumping at the
    /// touch to flatten. The standing instruction is to close small naked
    /// positions PROFITABLY; the second word is the operative one.
    #[tokio::test(start_paused = true)]
    async fn an_excess_kalshi_long_is_held_when_the_bid_is_under_its_basis() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("casebhold");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "other"));
        let pm = || Ok(net(&[("p-a", -2.0)]));
        let ka = || Ok(net(&[("K-a", 5.0)]));
        let k = FakeKalshi::new(vec![ka(), ka()]);
        let p = Arc::new(FakePmus(Mutex::new(vec![pm(), pm(), pm(), pm()])));
        // Bid 0.10 against a ~0.20 basis: selling here realises a loss.
        *k.quote.lock().expect("quote") = Ok(quote("active", "0.10", "0.12"));
        two_cycles(&mut st, &k, &p).await;
        assert!(k.placed().is_empty(), "waiting is the policy; flattening is not");
        assert_eq!(naked(), 1, "and the exposure stays reported while we wait");
    }

    /// SHADOW. Everything above the wire runs — both venue reads, the ownership
    /// contract, the ledger basis, the limit — and the order is printed instead
    /// of sent. The alternative is that the first time this prices a real order
    /// is also the first time anyone has watched it price one.
    #[tokio::test(start_paused = true)]
    async fn a_shadow_run_decides_everything_and_places_nothing() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("shadow");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(true, ledger.clone(), probe_log(&d, "other"));
        let (k, p) = steady(2);
        two_cycles(&mut st, &k, &p).await;
        assert_eq!(
            k.quotes_asked.load(Ordering::Relaxed),
            1,
            "it really did price the hedge against the live book"
        );
        assert!(k.placed().is_empty(), "and stopped at the wire");
        assert_eq!(
            crate::ledger::read(&ledger).expect("readable").len(),
            1,
            "the ledger is untouched"
        );
    }

    /// GUARD 3, AT THE ORDER PATH. One sighting is never enough, and this is
    /// where that stops being a gauge and starts being money. The first cycle
    /// must not even ASK for a quote.
    #[tokio::test(start_paused = true)]
    async fn one_sighting_places_nothing_and_does_not_even_read_a_quote() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("oneshot");
        let mut st = Act::new(false, ledger_with_one_open_basket(&d), probe_log(&d, "other"));
        let (k, p) = steady(1);
        let (kd, pd) = (
            k.clone() as Arc<dyn crate::sink::OrderSink>,
            p.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a")]);
        cycle(&mut r, &kd, &pd, Some(&mut st)).await.expect("cycle 1");
        assert!(k.placed().is_empty(), "a first sighting is not evidence");
        assert_eq!(k.quotes_asked.load(Ordering::Relaxed), 0, "and costs no venue read");
    }

    /// THE DOUBLE HEDGE, at the seam that decides. An obligation this engine
    /// already owes on the same market stops the order — and the finding is
    /// still reported, because a naked leg nobody may act on is exactly the one
    /// a human needs to see.
    #[tokio::test(start_paused = true)]
    async fn a_market_the_engine_already_owes_a_hedge_on_is_not_placed_against() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::from(["K-a".to_string()]));
        let d = scratch("double");
        let mut st = Act::new(false, ledger_with_one_open_basket(&d), probe_log(&d, "other"));
        let (k, p) = steady(2);
        two_cycles(&mut st, &k, &p).await;
        assert!(k.placed().is_empty(), "two hedgers on one market is the incident");
        assert_eq!(naked(), 1, "and it is still REPORTED");
    }

    /// A research probe's market is theirs to manage. Reported, never completed
    /// — and the quote is not even fetched, because a probe's book is not ours
    /// to spend budget on.
    #[tokio::test(start_paused = true)]
    async fn a_probe_owned_slug_is_reported_and_never_completed() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("probe");
        // the probe claims the very slug this finding is about
        let mut st = Act::new(false, ledger_with_one_open_basket(&d), probe_log(&d, "p-a"));
        let (k, p) = steady(2);
        two_cycles(&mut st, &k, &p).await;
        assert!(k.placed().is_empty());
        assert_eq!(k.quotes_asked.load(Ordering::Relaxed), 0);
        assert_eq!(naked(), 1, "surfaced, as `arb_core::naked` requires");
    }

    /// `ProbeOwnership` is FAIL-OPEN by design, so a pass that can read NONE of
    /// its sources would answer "ours" to every probe position in the book. A
    /// sweep that PLACES has to check its sources loaded — `arb_core::naked`
    /// says so in as many words — and this is that check.
    #[tokio::test(start_paused = true)]
    async fn an_ownership_read_that_loaded_nothing_refuses_to_act_at_all() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("noown");
        let mut st = Act::new(
            false,
            ledger_with_one_open_basket(&d),
            vec![d.join("does-not-exist.jsonl").to_string_lossy().into_owned()],
        );
        let (k, p) = steady(2);
        two_cycles(&mut st, &k, &p).await;
        assert!(k.placed().is_empty(), "an ownership predicate that knows nothing is not a gate");
        assert_eq!(naked(), 1, "the READ is unaffected — only acting is refused");
    }

    /// A halted venue is parked, not re-sent into. 335 identical places over 31
    /// minutes on 2026-07-30 is what this exists to prevent, and the park is on
    /// the same backoff the engine's hedge retry uses.
    #[tokio::test(start_paused = true)]
    async fn a_halted_market_is_parked_and_the_next_cycle_does_not_re_send() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("halt");
        let mut st = Act::new(false, ledger_with_one_open_basket(&d), probe_log(&d, "other"));
        let (k, p) = steady(3);
        *k.place.lock().expect("place") = Err(VenueError::Status {
            endpoint: "kalshi place",
            status: 409,
            body: r#"{"error":{"code":"trading_is_paused","message":"trading is paused"}}"#
                .into(),
        });
        two_cycles(&mut st, &k, &p).await;
        assert_eq!(k.placed().len(), 1, "it tried once");
        assert!(st.is_parked("K-a"), "and the venue's refusal bought a park");

        // a third cycle, inside the park, sends nothing
        let (kd, pd) = (
            k.clone() as Arc<dyn crate::sink::OrderSink>,
            p.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a")]);
        r.step(&net(&[("K-a", 2.0)]), &net(&[("p-a", -5.0)]), &|_| false);
        cycle(&mut r, &kd, &pd, Some(&mut st)).await.expect("cycle 3");
        assert_eq!(k.placed().len(), 1, "the park held");
        assert_eq!(
            k.quotes_asked.load(Ordering::Relaxed),
            1,
            "and it did not spend a quote read on a market that is not trading"
        );
    }

    /// A venue that ACCEPTED the order and then could not tell us what happened
    /// to it is the worst outcome available, and it must not be booked either
    /// way. `filled_qty` refuses rather than answering 0 exactly so that this
    /// case is reachable.
    #[tokio::test(start_paused = true)]
    async fn a_fill_we_could_not_read_is_never_booked() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("unreadable");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "other"));
        let (k, p) = steady(2);
        *k.filled.lock().expect("filled") = Err(VenueError::Transport("timed out".into()));
        let before = (act_unresolved(), act_refused());
        two_cycles(&mut st, &k, &p).await;
        assert_eq!(k.placed().len(), 1, "the order really went out");
        let booked = crate::ledger::read(&ledger).expect("readable");
        assert!(
            booked
                .iter()
                .all(|r| r.get("source").and_then(|v| v.as_str())
                    != Some(crate::ledger::SOURCE)),
            "an unreadable fill is not a fill: {booked:?}"
        );
        // ...and it lands in its OWN counter. An order the venue took whose fate
        // we cannot read is an open question about real money — folding it in
        // with "the ask was too dear" would bury the one number that must stay
        // 0 under the one that is expected to climb.
        assert_eq!(
            act_unresolved() - before.0,
            1,
            "positions_recon_act_unresolved is the must-stay-0 gauge"
        );
        assert_eq!(act_refused(), before.1, "and this is NOT a refusal — nothing was refused");
    }

    /// A venue reporting MORE filled than we ordered books only what we owed —
    /// booking the excess would invent a basket with no other leg — and says so
    /// on the must-stay-0 gauge, because the excess contracts are real and
    /// invisible to every exposure fold.
    #[tokio::test(start_paused = true)]
    async fn an_over_fill_books_only_what_was_ordered_and_alarms_for_the_rest() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("overfill");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "other"));
        let (k, p) = steady(2);
        *k.filled.lock().expect("filled") = Ok(9);
        let before = act_unresolved();
        two_cycles(&mut st, &k, &p).await;
        assert_eq!(k.placed()[0].qty, 3, "we asked for 3");
        let booked = crate::ledger::read(&ledger).expect("readable");
        let ours: Vec<_> = booked
            .iter()
            .filter(|r| r.get("source").and_then(|v| v.as_str()) == Some(crate::ledger::SOURCE))
            .collect();
        assert_eq!(ours[0]["qty"], 3, "and book 3, not the venue's 9");
        assert_eq!(
            act_unresolved() - before,
            1,
            "the 6 contracts nothing is booked against must not be silent"
        );
    }

    /// A place the VENUE refused is a non-event; a place that never got an
    /// answer is not. The second may be resting at the venue right now, and the
    /// two must not share a counter — the engine's own path has
    /// `place_answer_was_lost` and a `recover_place` behind it, and this pass
    /// has neither.
    #[tokio::test(start_paused = true)]
    async fn a_place_that_never_completed_is_unresolved_not_refused() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("lostplace");
        let mut st = Act::new(false, ledger_with_one_open_basket(&d), probe_log(&d, "other"));

        // A venue that ANSWERED no: an ordinary refusal.
        let (k, p) = steady(2);
        *k.place.lock().expect("place") = Err(VenueError::Status {
            endpoint: "kalshi place",
            status: 400,
            body: r#"{"error":{"code":"insufficient_balance"}}"#.into(),
        });
        let before = (act_refused(), act_unresolved());
        two_cycles(&mut st, &k, &p).await;
        assert!(act_refused() > before.0, "the venue said no, and that is all it is");
        assert_eq!(act_unresolved(), before.1, "nothing is outstanding");

        // A request that never completed: it may be resting at the venue.
        let (k, p) = steady(2);
        *k.place.lock().expect("place") = Err(VenueError::Transport("timed out".into()));
        let before = act_unresolved();
        let mut st2 = Act::new(false, ledger_with_one_open_basket(&d), probe_log(&d, "other"));
        two_cycles(&mut st2, &k, &p).await;
        assert_eq!(
            act_unresolved() - before,
            1,
            "a timeout is indistinguishable from an order whose answer was lost"
        );
    }

    /// An IOC that did not fill books nothing and is not an error — the book
    /// moved, and the next cycle reconsiders it.
    #[tokio::test(start_paused = true)]
    async fn an_unfilled_ioc_books_nothing() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("unfilled");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "other"));
        let (k, p) = steady(2);
        *k.filled.lock().expect("filled") = Ok(0);
        two_cycles(&mut st, &k, &p).await;
        let booked = crate::ledger::read(&ledger).expect("readable");
        assert_eq!(booked.len(), 1, "the original basket, and nothing else: {booked:?}");
    }

    /// A ledger that cannot be read is exposure that cannot be seen, so nothing
    /// is priced off it. This is the same STRICT read that gates arming, and the
    /// lenient one is not merely less informative — it is wrong in the expensive
    /// direction.
    ///
    /// The file below holds a good open basket and a TORN UNWIND that closes it.
    /// Skip-the-bad-lines leniency keeps the open record and drops the record
    /// that says it is gone, so the pass would price a completion against a
    /// position we no longer hold. Refusing the whole read is the only answer
    /// that cannot do that.
    #[tokio::test(start_paused = true)]
    async fn a_torn_ledger_line_stops_the_act_pass_rather_than_being_skipped() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("badledger");
        let ledger = ledger_with_one_open_basket(&d);
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new().append(true).open(&ledger).expect("open");
            // The unwind that closes it, torn by a crash mid-write.
            write!(f, "{{\"ts\":2.0,\"relationship_id\":\"a\",\"status\":\"unwo")
                .expect("torn");
        }
        let mut st = Act::new(false, ledger, probe_log(&d, "other"));
        let (k, p) = steady(2);
        two_cycles(&mut st, &k, &p).await;
        assert!(
            k.placed().is_empty(),
            "a ledger with one unreadable line is a ledger we cannot price from"
        );
        assert_eq!(naked(), 1, "the reconciliation still reports");
    }

    /// THE SINGLE-AUTHOR RULE, at the one moment it can still bite. The
    /// pre-flight contest check reads the ledger at the START of the cycle; the
    /// frozen Python hedger writes its own basket while our place is on the
    /// wire, which is exactly the 600ms of 2026-07-30. `append_basket` is what
    /// catches that, a raw `append` is not, and the difference is a
    /// `contested_with_ts` on the record plus the line that tells an operator to
    /// stop the timer.
    #[tokio::test(start_paused = true)]
    async fn a_writer_that_books_while_our_order_is_on_the_wire_is_caught_at_the_book() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        crate::naked_act::publish_inflight(BTreeSet::new());
        let d = scratch("contest");
        let ledger = ledger_with_one_open_basket(&d);
        let mut st = Act::new(false, ledger.clone(), probe_log(&d, "other"));
        let (k, p) = steady(2);
        // The Python's own record shape: same relationship, same two markets,
        // no `source` of ours, `ts` in the same instant as the one we are about
        // to write. It lands AFTER our pre-flight read.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs_f64();
        *k.write_on_place.lock().expect("race") = Some((
            ledger.clone(),
            format!(
                r#"{{"ts":{now},"relationship_id":"a","status":"open","qty":3,"legs":[
                   {{"venue":"kalshi","market_id":"K-a","side":"yes","qty":3,"yes_price":"0.19"}},
                   {{"venue":"polymarket_us","market_id":"p-a","side":"no","qty":3,
                     "yes_price":"0.22"}}]}}"#
            )
            .replace('\n', "")
            .replace("                   ", ""),
        ));
        two_cycles(&mut st, &k, &p).await;

        let booked = crate::ledger::read(&ledger).expect("readable");
        let ours: Vec<_> = booked
            .iter()
            .filter(|r| r.get("source").and_then(|v| v.as_str()) == Some(crate::ledger::SOURCE))
            .collect();
        assert_eq!(ours.len(), 1, "our fill is still booked — the money was spent");
        assert!(
            ours[0].get("contested_with_ts").is_some(),
            "and it NAMES the other writer's record, which is the only trace a human has \
             that two owners bought the same leg: {:?}",
            ours[0]
        );
    }

    /// `--positions-recon` ON ITS OWN. No ledger, no quote, no order, and the
    /// gauges move exactly as they did — the detect-only path must not have
    /// acquired a single new side effect.
    #[tokio::test(start_paused = true)]
    async fn a_detect_only_cycle_touches_nothing_new() {
        let _s = crate::naked_act::TEST_SERIAL.lock().await;
        let _g = GAUGES.lock().await;
        let (k, p) = steady(2);
        let (kd, pd) = (
            k.clone() as Arc<dyn crate::sink::OrderSink>,
            p.clone() as Arc<dyn crate::sink::OrderSink>,
        );
        let mut r = Recon::new(vec![pair("a")]);
        cycle(&mut r, &kd, &pd, None).await.expect("cycle 1");
        cycle(&mut r, &kd, &pd, None).await.expect("cycle 2");
        assert_eq!(naked(), 1, "it still detects");
        assert!(k.placed().is_empty(), "and places nothing");
        assert_eq!(k.quotes_asked.load(Ordering::Relaxed), 0, "and reads no quote");
    }
}
