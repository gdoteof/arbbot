//! Whether the books are current enough to quote off at all.
//!
//! Two independent facts, in order of locality: whether the engine's own
//! subscription to the recorder can be trusted (`Link`, `resync_reason`), and
//! whether the recorder says the venue sockets can be (`feed_stale_reason` over
//! `required_feeds`). Both fail CLOSED — an absent answer is not a healthy
//! answer — because the alternative is quoting on a feed we cannot see.

use super::Engine;
use arb_core::clock::now_s as wall_now;
use arb_core::model::Venue;
use arb_core::quoter::Quoter;
use std::collections::HashMap;

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
///
/// This exclusion is also what makes the counter-differencing in
/// `feed_stale_reason` affordable. INTL logged 2,634 stale episodes in 9 days
/// (~293/day) against 263 ever for PM-US; making INTL critical would fire that
/// detector — and its sweep — roughly 290 times a day.
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
pub(super) fn required_feeds(by_market: &HashMap<(Venue, String), Vec<usize>>) -> Vec<String> {
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

/// The publisher's `stale_after_s`: how long a connection must be silent before
/// the recorder starts counting flagged seconds for it. Mirrored here for the
/// REASON TEXT ONLY — no decision reads it, so drift makes a log line slightly
/// wrong and changes nothing else. Without it "stale for 2s" reads as a 2-second
/// blip when the socket was actually dead for at least twelve.
const RECORDER_STALE_AFTER_S: f64 = 10.0;

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
///
/// `seen` carries the previous tick's `stale_seconds_total` so that this is a
/// DETECTOR rather than a sampler. `stale` is an INSTANTANEOUS predicate — "this
/// feed has been silent for more than 10s" — recomputed once a second
/// (`arb-recorder/src/health.rs`, `record/recorder.py`), and this reads it every
/// five, so an outage of D seconds is flagged on `max(0, D-10)` lines and
/// observed with probability `min(1, (D-10)/5)`, decided by nothing but tick
/// phase: a 12s outage was caught 40% of the time. `stale_seconds_total` counts
/// those SAME flagged seconds cumulatively in the SAME line, so differencing it
/// catches every one of them at any phase. `data/health.jsonl` carried
/// `polymarket_us-ws: 2.0` — a 2-line episode on the order-carrying feed with a
/// ~60% chance of never being seen — and was watched ticking 2.0 -> 4.0 live on
/// 2026-07-29. `feed_iv` is `MissedTickBehavior::Skip`, so a skipped tick loses
/// the flag outright while the counter still covers the gap.
///
/// COST, because this is not free: differencing the whole live file finds 263
/// `polymarket_us-ws` episodes (34 in the last 24h; 215 of them 2s, longest 7s)
/// against ONE `kalshi-ws` episode ever. The sampler saw roughly 15 of
/// yesterday's 34; this sees all 34, so ~19 extra `pull_quotes` + `SweepAndVerify`
/// cycles a day, each costing maker queue position on every relationship and
/// drawing on a shared venue API budget with a 429 history.
fn feed_stale_reason(
    path: &str,
    now_wall: f64,
    required: &[String],
    seen: &mut HashMap<String, f64>,
) -> Option<String> {
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
    // Both recorders emit this object on EVERY line — empty when nothing has
    // ever been stale — so a line without it is a publisher whose semantics we
    // cannot assume, and quietly falling back to the flag alone would restore
    // the sampler this exists to replace. Missing evidence pulls, as everywhere
    // else here.
    let Some(totals) = v.get("stale_seconds_total").and_then(|t| t.as_object()) else {
        return Some(format!("health file {path} reports no stale_seconds_total"));
    };
    let stale = v.get("stale");
    let bad: Vec<String> = required
        .iter()
        .filter_map(|f| {
            // Absent from the COUNTER means "never stale in this recorder
            // process": the recorder inserts a key only once it has counted a
            // stale second, and the live file names 2 of the 3 feeds it
            // watches. That is the OPPOSITE of the rule for `stale` below, and
            // it is only safe because the object's own presence was just
            // required above.
            let total = totals.get(f).and_then(|n| n.as_f64()).unwrap_or(0.0);
            let missed = match seen.insert(f.clone(), total) {
                // Backwards: the recorder restarted and began counting again
                // from zero into the same append-only file, so a naive
                // difference would go negative. Everything the NEW counter
                // holds accumulated AFTER that restart, so it is all news and
                // all of it is reported. Zero — a recorder that has counted
                // nothing yet — is the ordinary case and reports nothing, which
                // is why a restart is not a false alarm; `45 -> 3` is the case
                // that must not be swallowed, and would be by any rule of the
                // form "report only what exceeds the last reading".
                Some(prev) if total < prev => total,
                Some(prev) => total - prev,
                // First reading: nothing to difference against. Whatever the
                // counter already holds happened before this engine started.
                None => 0.0,
            };
            match stale.and_then(|s| s.get(f)).and_then(|b| b.as_bool()) {
                Some(true) => Some(format!("{f} stale")),
                // Stale and recovered BETWEEN two ticks. This is NOT protection
                // DURING the blackout and must not be read as such: the counter
                // is differenced up to 5s after the episode ended, and it ended
                // because messages resumed, which repairs the book in
                // milliseconds. A 12s PM-US blackout gets zero seconds of cover
                // while it is happening.
                //
                // What it buys is the SWEEP. `feed_tick` sweeps on the way in,
                // so the quotes that were priced off a book we could not see get
                // cancelled and the venue book re-proved — which is the half
                // that matters in a system with this one's naked-fill history.
                Some(false) if missed > 0.0 => Some(format!(
                    "{f} stale for {missed:.0}s flagged (>={:.0}s silent) since the last check",
                    missed + RECORDER_STALE_AFTER_S
                )),
                Some(false) => None,
                // No entry is not a healthy entry. The recorder reporting nothing
                // about a venue we trade is indistinguishable, from here, from that
                // venue's socket being half-open — so it reads the same way.
                None => Some(format!("{f} unreported by the recorder")),
            }
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
pub(super) enum Link {
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
pub(super) fn resync_reason(link: &Link, now: std::time::Instant) -> Option<String> {
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
pub(super) struct FeedTick {
    /// The reason to stay pulled, or `None` to quote.
    pub(super) reason: Option<String>,
    /// The reason CHANGED — say it out loud.
    pub(super) log: bool,
    /// The engine has just gone from quoting to pulled: cancel every quote and
    /// PROVE the venue books empty.
    pub(super) sweep: bool,
    /// The subscription has proven itself; stop re-deriving that.
    pub(super) proven: bool,
}

/// The feed gate for one tick. `link_reason` is `resync_reason`, `health_reason`
/// is `feed_stale_reason` (`None` when there is no health file to read).
///
/// Extracted because the two rules that matter here were untestable inside a
/// `tokio::select!` over live channels, and one of them was simply missing: the
/// pull cancelled the quotes the engine still held ids for and swept nothing, so
/// `feed_pulled: true` could sit over real orders resting on a book the engine
/// could no longer see (C4(c)).
pub(super) fn feed_tick(
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

impl Engine {
    /// One feed-health tick: decide with `feed_tick`, then act on it.
    pub(super) fn health_tick(&mut self, quoters: &mut [Quoter]) {
        let t = feed_tick(
            self.feed_reason.as_ref(),
            resync_reason(&self.link, std::time::Instant::now()),
            match self.cfg.health_file.as_deref() {
                Some(p) => {
                    feed_stale_reason(p, wall_now(), &self.required, &mut self.stale_seen)
                }
                None => None,
            },
        );
        if t.proven {
            self.link = Link::Fresh; // stop re-deriving it
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
            self.pull_quotes(quoters, "FEED STALE");
        }
        self.feed_reason = t.reason;
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

    /// The recorder appends; so does this.
    fn append(path: &str, line: &str) {
        let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        writeln!(f, "{line}").unwrap();
    }

    const NOW: f64 = 1_000_000.0;

    fn line(ts: f64, kalshi: bool, pmus: bool) -> String {
        counted(ts, kalshi, pmus, "{}")
    }

    /// `line`, plus an explicit `stale_seconds_total` object.
    fn counted(ts: f64, kalshi: bool, pmus: bool, totals: &str) -> String {
        format!(
            r#"{{"ts":{ts},"stale":{{"kalshi-ws":{kalshi},"polymarket_us-ws":{pmus},"polymarket-ws":true}},"stale_seconds_total":{totals}}}"#
        )
    }

    /// `feed_stale_reason` with a throwaway counter baseline, for the cases
    /// where only the newest line matters.
    fn reason(path: &str, now: f64, required: &[String]) -> Option<String> {
        feed_stale_reason(path, now, required, &mut HashMap::new())
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
        assert_eq!(reason(&p, NOW, &required(&QUOTED)), None);
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
        assert_eq!(reason(&p, NOW, &required(&QUOTED)), None);
        let with_intl = required(&[Venue::Kalshi, Venue::PolymarketUs, Venue::Polymarket]);
        assert_eq!(with_intl, vec!["kalshi-ws", "polymarket_us-ws"], "INTL is data-only here");
        assert_eq!(reason(&p, NOW, &with_intl), None);
    }

    #[test]
    fn either_critical_feed_pulls_all_quotes() {
        for (k, pm, want) in [(true, false, "kalshi-ws"), (false, true, "polymarket_us-ws")] {
            let (_d, p) = health_file(&[&line(NOW - 1.0, k, pm)]);
            let why = reason(&p, NOW, &required(&QUOTED)).expect("must pull");
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
            r#"{{"ts":{},"stale":{{"polymarket_us-ws":false,"polymarket-ws":false}},"stale_seconds_total":{{}}}}"#,
            NOW - 1.0
        );
        let (_d, p) = health_file(&[&l]);
        let why = reason(&p, NOW, &required(&QUOTED)).expect("an unreported feed must pull");
        assert!(why.contains("kalshi-ws"), "{why}");
        assert!(why.contains("unreported"), "{why}");
        // ...and a `stale` object that is missing entirely is not a clean bill
        // of health either, counter or no counter.
        append(&p, &format!(r#"{{"ts":{},"stale_seconds_total":{{}}}}"#, NOW - 1.0));
        assert!(reason(&p, NOW, &required(&QUOTED)).is_some());
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
        let l = format!(
            r#"{{"ts":{},"stale":{{"kalshi-ws":false}},"stale_seconds_total":{{}}}}"#,
            NOW - 1.0
        );
        let (_d, p) = health_file(&[&l]);
        assert_eq!(
            reason(&p, NOW, &required(&[Venue::Kalshi])),
            None,
            "an absent PM-US entry cannot pull an engine that quotes no PM-US market"
        );
        // ...and the same file DOES pull once PM-US is quoted.
        let why = reason(&p, NOW, &required(&QUOTED)).expect("must pull");
        assert!(why.contains("polymarket_us-ws"), "{why}");
    }

    /// THE POINT. `stale` is an INSTANTANEOUS predicate the recorder recomputes
    /// every second and this check reads every five, which makes reading it
    /// alone a SAMPLER: an outage of D seconds is flagged on `max(0, D-10)`
    /// lines and observed with probability `min(1, (D-10)/5)`, decided by
    /// nothing but tick phase. D <= 10s is never seen (recorder policy);
    /// 10 < D < 15s is a coin flip — a 12s outage was caught 40% of the time;
    /// D >= 15s is always seen, up to 5s late on top of the recorder's own 10s.
    ///
    /// LIVE: `data/health.jsonl` carried `polymarket_us-ws: 2.0` — two flagged
    /// seconds on the order-carrying feed, an episode with roughly a 60% chance
    /// of never being observed — and that counter was watched ticking
    /// 2.0 -> 4.0 on 2026-07-29 while the flag was down at both ticks either
    /// side of it.
    ///
    /// `stale_seconds_total` counts those same flagged seconds cumulatively in
    /// the SAME line, so differencing it sees every one of them at any phase.
    #[test]
    fn a_stale_episode_shorter_than_the_tick_interval_is_still_seen() {
        let req = required(&QUOTED);
        let mut seen = HashMap::new();
        let (_d, p) =
            health_file(&[&counted(NOW - 1.0, false, false, r#"{"polymarket_us-ws":2.0}"#)]);
        assert_eq!(
            feed_stale_reason(&p, NOW, &req, &mut seen),
            None,
            "baseline tick, all flags down"
        );
        // Five seconds later the flag is down AGAIN: the whole two-second
        // episode fell between the ticks and `stale` never showed it.
        append(&p, &counted(NOW + 4.0, false, false, r#"{"polymarket_us-ws":4.0}"#));
        let why = feed_stale_reason(&p, NOW + 5.0, &req, &mut seen).expect("the counter moved");
        assert!(why.contains("polymarket_us-ws"), "{why}");
        // Both numbers: 2 FLAGGED seconds is at least 12 seconds of dead
        // socket, because the recorder counts nothing for the first 10.
        assert!(why.contains("2s flagged"), "{why}");
        assert!(why.contains(">=12s silent"), "{why}");
        // ...and it clears on the next tick, once the counter stops advancing.
        append(&p, &counted(NOW + 9.0, false, false, r#"{"polymarket_us-ws":4.0}"#));
        assert_eq!(feed_stale_reason(&p, NOW + 10.0, &req, &mut seen), None);
    }

    /// The counter is monotone only WITHIN a recorder process: a restart begins
    /// it again at zero in the same append-only file, and the live file holds 24
    /// such resets (4 on `polymarket_us-ws`), so this path is exercised. A naive
    /// difference goes negative there, and reading "not greater than last time"
    /// as healthy would swallow every real stale second until the new counter
    /// passed the old one — a false all-clear of the same shape as
    /// `unwrap_or(false)`.
    ///
    /// So a backwards step reports the NEW total: everything it holds
    /// accumulated after the restart. `45 -> 0` reports nothing, because a
    /// recorder that has counted nothing has nothing to report — that is the
    /// ordinary shape and it is not a false alarm. `45 -> 3` reports 3, and is
    /// the shape a rebaseline-and-stay-quiet rule would have swallowed.
    ///
    /// Deliberately NOT relying on the belt-and-braces: today a restart also
    /// takes the engine's subscription down (`Link::Down`, which outranks the
    /// file) because ONE process both writes health.jsonl and serves the socket
    /// the trader attaches to (`config/recorder.yaml`). Split those, or point
    /// the trader at one recorder's socket and another's health file, and that
    /// cover is gone — so the rule above has to stand on its own.
    #[test]
    fn a_recorder_restart_is_neither_a_false_alarm_nor_a_false_all_clear() {
        let req = required(&QUOTED);
        let mut seen = HashMap::new();
        let (_d, p) =
            health_file(&[&counted(NOW - 1.0, false, false, r#"{"polymarket_us-ws":45.0}"#)]);
        assert_eq!(feed_stale_reason(&p, NOW, &req, &mut seen), None);
        // Restarted: counters back to zero, both feeds reporting healthy.
        append(&p, &counted(NOW + 4.0, false, false, "{}"));
        assert_eq!(
            feed_stale_reason(&p, NOW + 5.0, &req, &mut seen),
            None,
            "45 -> 0 is a restart, not 45 seconds of staleness"
        );
        // ...and the baseline moved with it, so ONE flagged second after the
        // restart still pulls instead of being swallowed until the counter
        // climbs back past 45.
        append(&p, &counted(NOW + 9.0, false, false, r#"{"polymarket_us-ws":1.0}"#));
        let why = feed_stale_reason(&p, NOW + 10.0, &req, &mut seen).expect("must pull");
        assert!(why.contains("polymarket_us-ws"), "{why}");
        // ...and the restart that lands on a tick ALREADY carrying flagged
        // seconds — 45 -> 3, below the old baseline but not zero — reports all
        // three rather than dropping them.
        let mut seen2 = HashMap::new();
        append(&p, &counted(NOW + 14.0, false, false, r#"{"polymarket_us-ws":45.0}"#));
        assert_eq!(feed_stale_reason(&p, NOW + 15.0, &req, &mut seen2), None);
        append(&p, &counted(NOW + 19.0, false, false, r#"{"polymarket_us-ws":3.0}"#));
        let why = feed_stale_reason(&p, NOW + 20.0, &req, &mut seen2).expect("must pull");
        assert!(why.contains("3s flagged"), "45 -> 3 must not be swallowed: {why}");
    }

    /// The counter's absent-KEY rule is the opposite of `stale`'s, and has to
    /// be: `stale_seconds_total` omits every feed that has never been stale —
    /// the live file names 2 of the 3 feeds it watches — so an absent key means
    /// zero, not missing evidence.
    ///
    /// That inversion is only safe because the OBJECT's presence is required.
    /// Both recorders write it on every line, empty when nothing has ever been
    /// stale, so a line without it is a publisher whose semantics we cannot
    /// assume — and degrading silently back to the flag alone would restore the
    /// sampler above.
    #[test]
    fn an_absent_counter_key_means_never_stale_but_an_absent_counter_object_pulls() {
        let req = required(&QUOTED);
        let mut seen = HashMap::new();
        // kalshi-ws is flagged healthy and simply not counted: never stale.
        let (_d, p) =
            health_file(&[&counted(NOW - 1.0, false, false, r#"{"polymarket_us-ws":2.0}"#)]);
        assert_eq!(feed_stale_reason(&p, NOW, &req, &mut seen), None);
        assert_eq!(seen.get("kalshi-ws"), Some(&0.0), "an absent key baselines at zero");
        // The object missing entirely is missing evidence, not a clean bill.
        let no_counter = format!(
            r#"{{"ts":{},"stale":{{"kalshi-ws":false,"polymarket_us-ws":false}}}}"#,
            NOW - 1.0
        );
        append(&p, &no_counter);
        let why = feed_stale_reason(&p, NOW, &req, &mut seen).expect("must pull");
        assert!(why.contains("stale_seconds_total"), "{why}");
    }

    /// The health writer going quiet means the recorder is down — worse than
    /// any single feed, and the flags in the last line are stale evidence.
    #[test]
    fn a_silent_recorder_is_stale_even_when_the_last_line_looked_healthy() {
        let (_d, p) = health_file(&[&line(NOW - 120.0, false, false)]);
        let why = reason(&p, NOW, &required(&QUOTED)).expect("must pull");
        assert!(why.contains("recorder silent"), "{why}");
    }

    /// Only the LAST line counts; an old healthy line must not rescue a new
    /// stale one.
    #[test]
    fn only_the_last_line_is_read() {
        let (_d, p) = health_file(&[&line(NOW - 5.0, false, false), &line(NOW - 1.0, true, false)]);
        assert!(
            reason(&p, NOW, &required(&QUOTED)).is_some(),
            "the newest line is stale"
        );
    }

    /// FAIL-CLOSED: no file, no readable line, or garbage all pull the quotes.
    /// Python left the state unchanged here, which would quote forever on a
    /// feed it could not see.
    #[test]
    fn an_unreadable_health_file_pulls_quotes() {
        let req = required(&QUOTED);
        assert!(reason("/nonexistent/health.jsonl", NOW, &req).is_some());
        let (_d, p) = health_file(&["not json at all"]);
        assert!(reason(&p, NOW, &req).is_some());
        let (_d2, p2) = health_file(&[]);
        assert!(reason(&p2, NOW, &req).is_some(), "an empty file proves nothing");
    }

    /// A line with no `ts` is treated as infinitely old, not as ts=now.
    #[test]
    fn a_line_without_a_timestamp_is_stale() {
        let (_d, p) = health_file(&[r#"{"stale":{"kalshi-ws":false,"polymarket_us-ws":false}}"#]);
        assert!(reason(&p, NOW, &required(&QUOTED)).is_some());
    }

    /// The tail window can start mid-codepoint; that must not panic or hide a
    /// healthy line.
    #[test]
    fn a_large_file_reads_only_its_tail() {
        let pad = format!(r#"{{"ts":1,"note":"{}"}}"#, "é".repeat(3000));
        let (_d, p) = health_file(&[&pad, &line(NOW - 1.0, false, false)]);
        assert_eq!(reason(&p, NOW, &required(&QUOTED)), None);
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
