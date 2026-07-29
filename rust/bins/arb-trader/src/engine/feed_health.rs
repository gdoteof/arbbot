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
            self.cfg
                .health_file
                .as_deref()
                .and_then(|p| feed_stale_reason(p, wall_now(), &self.required)),
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
