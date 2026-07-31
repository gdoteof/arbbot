//! Venue-truth positions reconciliation. DETECTION AND REPORTING ONLY, and OFF
//! unless `--positions-recon` is passed.
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
//! WHAT IT IS NOT. It does not place, size, price or book anything, and it does
//! not feed the hedge path. There is deliberately no armed spelling of the flag
//! (the same posture as `--unwind-detect-only`). Two hedgers on one Kalshi key
//! is not redundancy, it is a DOUBLE HEDGE — `orphan.rs` works that through:
//! both IOCs fill, the account ends long 2 against a short 1, and
//! `hedges_overfilled` cannot even see it because the other party's fill is
//! credited to no obligation of ours. Acting stays with the single owner that
//! already has it until somebody decides otherwise, in a reviewed change, with
//! that ownership settled first.
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

pub fn naked() -> i64 {
    NAKED.load(Ordering::Relaxed)
}
pub fn unconfirmed() -> i64 {
    UNCONFIRMED.load(Ordering::Relaxed)
}
pub fn failures() -> u64 {
    FAILURES.load(Ordering::Relaxed)
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

/// One reconciliation cycle. `Err` = the cycle was abandoned; NOTHING is
/// concluded from it, and in particular no gauge moves except the failure
/// counter.
async fn cycle(
    recon: &mut Recon,
    kalshi: &Arc<dyn crate::sink::OrderSink>,
    pmus: &Arc<dyn crate::sink::OrderSink>,
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
    Ok(())
}

/// The periodic loop. Runs until the process exits.
pub async fn recon_loop(
    mut recon: Recon,
    kalshi: Arc<dyn crate::sink::OrderSink>,
    pmus: Arc<dyn crate::sink::OrderSink>,
) {
    // `tokio::time::interval`'s first tick is ready immediately, and that is
    // wanted: the first cycle takes the baseline snapshot that the second can
    // confirm against, so an orphan this process inherited is reported five
    // minutes in rather than ten.
    let mut iv = tokio::time::interval(INTERVAL);
    loop {
        iv.tick().await;
        if let Err(e) = cycle(&mut recon, &kalshi, &pmus).await {
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
    static GAUGES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        let e = cycle(&mut r, &kd, &pd).await.expect_err("a 503 is not an empty portfolio");
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
        cycle(&mut r, &kd, &pd).await.expect("clean");
        cycle(&mut r, &kd, &pd).await.expect("clean");
        assert_eq!(naked(), 1);

        let dead = Reads::new(vec![boom(), boom(), boom()]) as Arc<dyn crate::sink::OrderSink>;
        let idle = Reads::new(vec![]) as Arc<dyn crate::sink::OrderSink>;
        assert!(cycle(&mut r, &idle, &dead).await.is_err());
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
        cycle(&mut r, &kd, &pd).await.expect("clean");
        assert_eq!(naked(), 0, "one sighting is not a report");
        cycle(&mut r, &kd, &pd).await.expect("clean");
        assert_eq!(naked(), 1);
        // Wall-clock, not the paused runtime clock, so assert the property
        // (a completed cycle is fresh) rather than an exact second.
        assert!((0..5).contains(&age_s()), "a completed cycle is fresh: {}", age_s());
    }
}
