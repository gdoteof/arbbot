//! The stage a tape diff cannot be: a REAL subscriber on the recorder's socket,
//! reading it under CPU contention.
//!
//! WHY THIS EXISTS. The 2026-07-29 recorder hang appeared only under CPU
//! contention and passed nine runs out of ten (task #48). A gate that compares
//! finished tape files would have been green through every sighting, because a
//! tape file says nothing about whether the process producing it is still
//! serving anyone. The other half of the same blind spot is that the Rust
//! recorder has run as a WRITE-ONLY shadow since it was built: `data/arbbot-rs.
//! sock` has had LISTEN and zero connected peers throughout, so its entire
//! consumption path — welcome burst, fan-out, eviction — has never been
//! exercised by anything. Both are the same gap, and one subscriber under load
//! closes both.
//!
//! WHAT IS CHECKED. The first three are lines from `docs/migration-plan.md`'s
//! M1 gate; the fourth and the welcome-coverage check below are additions.
//!  * the stream survives the window with no disconnect ("zero unexplained
//!    disconnects") — an EOF here IS the broadcaster's eviction, which is
//!    silent on the client and only says so on the recorder's stderr (PR #27,
//!    `47e3c9b`);
//!  * no `GapDetected` ("gap counter"). The sequence a subscriber receives is
//!    the recorder's own per-market +1 (`SeqCounter::next`), not the venue's,
//!    so a hole is never a venue renumbering or a resubscribe. It is ONE of two
//!    things, and the claim that it could only be the first was retracted from
//!    here: either a frame the fan-out numbered and this subscriber never got,
//!    or a number the recorder SPENT on a call that then failed —
//!    `pmintl.rs:344`, `kalshi.rs:304`, `pmus.rs:218` all bump the counter
//!    before a fallible `.await` and drop it on `Err`. That second reading is
//!    the recorder defect filed separately; the long comment in `main.rs`'s
//!    tape gating carries the measurement and the way to tell the two apart.
//!    What no reading of it can see is loss UPSTREAM of the numbering — a
//!    message the venue sent and the recorder never received — because the +1
//!    counter closes over it. That direction is the recorder's own `[hb] gaps=`
//!    counter, read by hand in §1 of the runbook;
//!  * no `NotSynced` — a delta for a market whose snapshot never arrived is a
//!    HOLE IN THE WELCOME BURST, which is exactly the loss PR #27 fixed and the
//!    only way to see it from outside is to be a subscriber;
//!  * no `bad_field`, the same decimal check the tape side does.
//!
//! LOAD IS BOUNDED AND IT IS BOUNDED FOR A REASON. The live feed degraded on
//! 2026-07-29 when a load spike starved it; the Python recorder that the ARMED
//! engine is subscribed to runs at Nice=0 while these workers run at the
//! gate's own niceness. `MAX_WORKERS` caps concurrency at 8 and `plan_workers`
//! refuses to start if the 1-minute load average plus the workers would cross
//! the ceiling. The reader re-reads the load every second and stops the workers
//! early if it crosses anyway.

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::model::{TapeEvent, Venue};
use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Never more than this many load workers, whatever is asked for.
pub const MAX_WORKERS: usize = 8;
/// 1-minute load average this gate will not push the box past.
pub const DEFAULT_LOAD_CEILING: f64 = 40.0;

/// This process's nice value, parsed out of a `/proc/<pid>/stat` line.
///
/// `Nice=19` is a property of the systemd UNIT and of nothing else. The binary
/// sets no priority of its own, and the runbook's pre-flight tells an operator
/// to invoke it by hand with `--load 6` — six unniced burners on the box
/// carrying the armed engine's feed, outranking even the Rust recorder (NI 10).
/// That is not a hypothetical: it was done on 2026-07-29 and it degraded the
/// live feed. So the gate reads its own priority and says so.
///
/// The parse is fiddly for one reason: field 2 is the executable name in
/// parentheses and may itself contain spaces and parentheses, so the only safe
/// split point is the LAST `)`. Fields after it start at 3, and nice is 19.
pub fn nice_from_stat(stat: &str) -> Option<i64> {
    let after = stat.rsplit_once(')')?.1;
    after.split_whitespace().nth(19 - 3)?.parse().ok()
}

pub fn own_nice() -> Option<i64> {
    nice_from_stat(&std::fs::read_to_string("/proc/self/stat").ok()?)
}

pub fn load1() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse().ok())
        .unwrap_or(f64::INFINITY)
}

/// How many CPU workers may run right now, or why none may.
///
/// A worker is one runnable thread, so it adds ~1.0 to the 1-minute average at
/// steady state; refusing when `load1 + workers` would cross the ceiling is the
/// arithmetic that keeps the promise rather than hoping for it.
pub fn plan_workers(requested: usize, load1: f64, ceiling: f64) -> Result<usize, String> {
    let workers = requested.min(MAX_WORKERS);
    if workers == 0 {
        return Ok(0);
    }
    if load1 + workers as f64 > ceiling {
        return Err(format!(
            "load average is {load1:.2}; {workers} workers would put it over the {ceiling:.0} \
             ceiling. Refusing — the armed engine's feed shares these CPUs."
        ));
    }
    Ok(workers)
}

/// Everything a subscriber can tell about the stream it was served.
#[derive(Default)]
pub struct StreamCheck {
    pub lines: u64,
    pub undecodable: u64,
    pub bad_field: u64,
    pub gaps: u64,
    pub unsynced: u64,
    pub snapshots: u64,
    pub deltas: u64,
    pub trades: u64,
    pub markets: HashSet<String>,
    /// Distinct markets in the LEADING run of snapshots — the welcome burst.
    pub welcome_markets: usize,
    /// The stream ended before we stopped reading: evicted, or the recorder died.
    pub disconnected: bool,
    welcome_open: bool,
    welcome_seen: HashSet<(Venue, String)>,
    books: BookBuilder,
}

/// Markets the Python recorder is actively publishing that the Rust recorder's
/// WELCOME BURST does not carry.
///
/// This is the universe check, and it lives here rather than on the tape side
/// on purpose. Comparing the two TAPES over a window answers the wrong
/// question. This used to say the reason was that "the Rust recorder drops
/// consecutive-identical snapshots"; it does not, and there is no dedup in the
/// recorder at all — `Core::on_event` writes every event it is handed. The
/// reason is that Rust PM-US is WS-EVENT-DRIVEN while Python POLLS, so a market
/// nothing happened to inside the window produces Rust lines only if the venue
/// pushed a frame, and Python lines regardless. Measured: 19.4% of consecutive
/// Python PM-US lines repeat their predecessor against 2.2% of Rust's. Censused
/// rather than sampled on 2026-07-29 — all 66 PM-US markets missing from a 900s
/// Rust window appear in the SAME DAY's Rust tape, between 15 and 8,558 times
/// each. Every one. Gating on that difference would make this gate cry wolf
/// every single day.
///
/// The welcome burst has far less ambiguity: it is one snapshot per book the
/// recorder is CURRENTLY TRACKING, so a market missing from it is a market the
/// Rust recorder does not have — which is the only version of this that a
/// subscriber would ever feel.
///
/// WHAT IT STILL DOES NOT SEE: a book the recorder is tracking but no longer
/// refreshing. `[pm-ws] resnapshot sweep: 3/111 books did NOT refresh — their
/// age is now unbounded` (live, eleven minutes after a restart) describes three
/// books that are in the welcome burst, pass this check, and are stale. Being
/// present is not being fresh, and this check only asks the first question.
pub fn welcome_coverage_gap(welcome: &HashSet<String>, python_window: &HashSet<String>) -> Vec<String> {
    let mut v: Vec<String> = python_window.difference(welcome).cloned().collect();
    v.sort();
    v
}

impl StreamCheck {
    pub fn new() -> Self {
        Self { welcome_open: true, ..Default::default() }
    }

    /// The welcome burst's markets for one venue.
    pub fn welcome_for(&self, venue: Venue) -> HashSet<String> {
        self.welcome_seen
            .iter()
            .filter(|(v, _)| *v == venue)
            .map(|(_, m)| m.clone())
            .collect()
    }

    pub fn feed(&mut self, line: &str) {
        self.lines += 1;
        let ev: TapeEvent = match serde_json::from_str(line) {
            Ok(ev) => ev,
            Err(_) => {
                self.undecodable += 1;
                return;
            }
        };
        if !crate::tape::decimal_fields_ok(&ev) {
            self.bad_field += 1;
        }
        match &ev {
            TapeEvent::Snapshot { venue, market_id, .. } => {
                self.snapshots += 1;
                if self.welcome_open {
                    self.welcome_seen.insert((*venue, market_id.clone()));
                    self.welcome_markets = self.welcome_seen.len();
                }
            }
            TapeEvent::Delta { .. } => {
                self.deltas += 1;
                self.welcome_open = false;
            }
            TapeEvent::Trade { .. } => {
                self.trades += 1;
                self.welcome_open = false;
            }
        }
        self.markets.insert(ev.market_id().to_owned());
        match self.books.apply_event(&ev) {
            Ok(()) => {}
            Err(ApplyError::GapDetected { .. }) => self.gaps += 1,
            Err(ApplyError::NotSynced) => self.unsynced += 1,
        }
    }

    /// The gating verdict. `Ok(())` or the first reason it is not.
    pub fn verdict(&self) -> Result<(), String> {
        if self.lines == 0 {
            return Err("the socket delivered NOTHING — no welcome burst at all".into());
        }
        if self.disconnected {
            return Err(
                "the stream ENDED before the window did. That is the broadcaster's eviction \
                 (silent on this side; the notice is on the recorder's stderr) or a recorder \
                 restart. A trader seeing this pulls every resting quote."
                    .into(),
            );
        }
        if self.welcome_markets == 0 {
            return Err("the stream opened with no snapshot — there was no welcome burst".into());
        }
        if self.unsynced > 0 {
            return Err(format!(
                "{} delta(s) arrived for a market with no snapshot: a HOLE in the welcome \
                 burst, which is the loss PR #27 exists to prevent",
                self.unsynced
            ));
        }
        if self.gaps > 0 {
            return Err(format!(
                "{} sequence gap(s) on the wire: either a frame this subscriber never got, or a \
                 number the recorder spent on a call that failed (pmintl.rs:344, kalshi.rs:304). \
                 Check the venue's resnapshot-failure line before reading it as loss",
                self.gaps
            ));
        }
        if self.undecodable > 0 {
            return Err(format!("{} line(s) the subscriber could not decode", self.undecodable));
        }
        if self.bad_field > 0 {
            return Err(format!(
                "{} line(s) carry a price/size that is not a number",
                self.bad_field
            ));
        }
        Ok(())
    }
}

/// Is the recorder that is RUNNING the recorder this gate just tested?
///
/// FOUND 2026-07-29, and it is the reason this check exists at all. The live
/// shadow recorder (started 01:53) was running an image that does not contain
/// the string `DROPPED subscriber` at all, while the binary on disk — rebuilt
/// at 14:02 — does. PR #27 (`47e3c9b`, 04:29) carries both the eviction notice
/// and the register-exactly-once welcome, and it landed two and a half hours
/// AFTER that process started, so the fix was never broken: it was not running.
/// It, and the book-eviction logging, were in the tree and NONE of them were in
/// the process producing the shadow evidence. A
/// subscriber attached to it was silently shed after ~95-155s with nothing in
/// the journal, exactly as an image with no notice in it would behave.
///
/// `cargo build` replacing the binary does not restart the unit, and
/// `Restart=always` only fires on exit, so a recorder can serve for days on an
/// image nobody has looked at. Everything else in this gate describes the TREE;
/// this is the only check that describes the PROCESS.
///
/// The kernel appends the literal " (deleted)" to `/proc/<pid>/exe` when the
/// executable's inode has been unlinked, which is exactly what a rebuild does.
pub fn running_image_verdict(link_target: &str) -> Result<(), String> {
    if link_target.ends_with(" (deleted)") {
        return Err(format!(
            "the running recorder's executable has been REPLACED on disk since it started \
             ({link_target}). It is serving an older image than the one this gate tested, so \
             nothing above describes the process that is actually running. Restart \
             arbbot-recorder-rs.service and re-run."
        ));
    }
    Ok(())
}

/// `/proc/<pid>/exe` for every process whose image is (or was) `exe`.
pub fn running_images(exe: &Path) -> Vec<String> {
    let want = exe.to_string_lossy().to_string();
    let Ok(rd) = std::fs::read_dir("/proc") else { return Vec::new() };
    rd.filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().chars().all(|c| c.is_ascii_digit()))
        .filter_map(|e| std::fs::read_link(e.path().join("exe")).ok())
        .map(|p| p.to_string_lossy().to_string())
        .filter(|p| p == &want || p == &format!("{want} (deleted)"))
        .collect()
}

pub struct LoadGuard {
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
    pub workers: usize,
}

impl LoadGuard {
    pub fn start(workers: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handles = (0..workers)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut x: u64 = 0;
                    while !stop.load(Ordering::Relaxed) {
                        for _ in 0..1_000_000 {
                            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                        }
                        std::hint::black_box(x);
                    }
                })
            })
            .collect();
        Self { stop, handles, workers }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct LiveResult {
    pub check: StreamCheck,
    pub workers: usize,
    pub load_start: f64,
    pub load_peak: f64,
    /// The load ceiling was crossed and the workers were stopped early.
    pub load_aborted: bool,
    pub elapsed_s: f64,
}

/// Attach for `seconds`, with `workers` CPU burners alongside.
///
/// Read-only in the strongest sense available: a subscriber never sends, and
/// this one drops its write half immediately.
pub fn attach(
    socket: &Path,
    seconds: u64,
    workers: usize,
    ceiling: f64,
) -> std::io::Result<LiveResult> {
    let load_start = load1();
    let mut guard = LoadGuard::start(workers);
    let mut load_peak = load_start;
    let mut load_aborted = false;
    let started = Instant::now();

    let stream = UnixStream::connect(socket)?;
    // A read timeout is what makes a SILENT recorder distinguishable from a
    // busy one: without it a wedged recorder parks this gate forever, which is
    // the failure mode the whole exercise is about.
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut reader = BufReader::new(stream);
    let mut check = StreamCheck::new();
    let mut line = String::new();
    let mut last_load = Instant::now();
    while started.elapsed() < Duration::from_secs(seconds) {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                check.disconnected = true;
                break;
            }
            Ok(_) => check.feed(line.trim_end()),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e),
        }
        if last_load.elapsed() >= Duration::from_secs(1) {
            last_load = Instant::now();
            let l = load1();
            load_peak = load_peak.max(l);
            if l > ceiling && guard.workers > 0 {
                load_aborted = true;
                guard.stop();
            }
        }
    }
    let workers = guard.workers;
    guard.stop();
    Ok(LiveResult {
        check,
        workers,
        load_start,
        load_peak,
        load_aborted,
        elapsed_s: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(mkt: &str, seq: u64) -> String {
        format!(
            r#"{{"kind":"snapshot","venue":"kalshi","market_id":"{mkt}","bids":[{{"price":"0.40","size":"10"}}],"asks":[{{"price":"0.60","size":"10"}}],"seq":{seq},"ts_local_ns":1,"ts_venue":null}}"#
        )
    }
    fn delta(mkt: &str, seq: u64) -> String {
        format!(
            r#"{{"kind":"delta","venue":"kalshi","market_id":"{mkt}","side":"bid","price":"0.41","size":"5","seq":{seq},"ts_local_ns":2,"ts_venue":null}}"#
        )
    }

    #[test]
    fn a_welcome_burst_then_contiguous_deltas_passes() {
        let mut c = StreamCheck::new();
        for m in ["A", "B", "C"] {
            c.feed(&snap(m, 1));
        }
        for seq in 2..=20 {
            c.feed(&delta("A", seq));
        }
        assert_eq!(c.welcome_markets, 3);
        assert_eq!(c.verdict(), Ok(()));
    }

    /// The welcome hole, seen from the only place it is visible: a market whose
    /// snapshot never arrived, whose deltas therefore have nothing to apply to.
    #[test]
    fn a_market_missing_from_the_welcome_burst_fails() {
        let mut c = StreamCheck::new();
        c.feed(&snap("A", 1));
        c.feed(&delta("B", 2));
        let e = c.verdict().expect_err("a delta with no snapshot must fail");
        assert!(e.contains("HOLE in the welcome burst"), "{e}");
    }

    #[test]
    fn a_sequence_hole_on_the_wire_fails() {
        let mut c = StreamCheck::new();
        c.feed(&snap("A", 1));
        c.feed(&delta("A", 2));
        c.feed(&delta("A", 9));
        let e = c.verdict().expect_err("a gap must fail");
        assert!(e.contains("sequence gap"), "{e}");
    }

    /// PR #27's territory. Eviction is SILENT on this side — the only thing the
    /// subscriber ever sees is the stream ending — so "ended early" has to be a
    /// failure in its own right or the gate cannot see an eviction at all.
    #[test]
    fn a_stream_that_ends_early_is_a_failure_not_a_clean_finish() {
        let mut c = StreamCheck::new();
        c.feed(&snap("A", 1));
        assert_eq!(c.verdict(), Ok(()));
        c.disconnected = true;
        let e = c.verdict().expect_err("an early EOF must fail");
        assert!(e.contains("ENDED before the window"), "{e}");
    }

    /// The only check here that is about the PROCESS rather than the tree.
    #[test]
    fn a_recorder_running_a_replaced_binary_is_a_failure() {
        assert_eq!(running_image_verdict("/x/arb-recorder"), Ok(()));
        let e = running_image_verdict("/x/arb-recorder (deleted)")
            .expect_err("a replaced image must fail");
        assert!(e.contains("REPLACED on disk"), "{e}");
        // and the scanner must find THIS test binary, deleted suffix or not,
        // or the check silently describes nothing
        let me = std::env::current_exe().expect("current exe");
        assert!(!running_images(&me).is_empty(), "did not find our own image at {me:?}");
        assert!(running_images(Path::new("/no/such/binary")).is_empty());
    }

    /// The gate is only niced when systemd nices it. A hand-run from the
    /// runbook is not, and six unniced burners next to the armed engine's feed
    /// is a thing that has already happened once.
    #[test]
    fn the_gate_can_read_its_own_priority() {
        // field 2 is allowed to contain spaces AND parentheses, which is why
        // the split is on the LAST `)` and not on the first.
        let stat = "42 (arb shadow (gate)) S 1 42 42 0 -1 4194304 100 0 0 0 5 6 0 0 20 19 4 0 \
                    900 0 0";
        assert_eq!(nice_from_stat(stat), Some(19));
        assert_eq!(nice_from_stat("nonsense with no paren"), None);
        // and it reads the real thing, or the parse describes nothing
        assert!(own_nice().is_some(), "could not read this process's own nice value");
    }

    #[test]
    fn the_load_planner_caps_workers_and_refuses_a_loaded_box() {
        assert_eq!(plan_workers(64, 1.0, DEFAULT_LOAD_CEILING), Ok(MAX_WORKERS));
        assert_eq!(plan_workers(4, 2.0, DEFAULT_LOAD_CEILING), Ok(4));
        let e = plan_workers(8, 35.0, DEFAULT_LOAD_CEILING).expect_err("must refuse");
        assert!(e.contains("Refusing"), "{e}");
        // exactly at the ceiling is allowed; one over is not
        assert_eq!(plan_workers(8, 32.0, 40.0), Ok(8));
        assert!(plan_workers(8, 32.1, 40.0).is_err());
    }

    /// The universe check, in the only place it can be answered without
    /// confusing an event-driven feed for a lossy one.
    #[test]
    fn the_welcome_burst_is_where_a_coverage_hole_is_visible() {
        let mut c = StreamCheck::new();
        c.feed(&snap("A", 1));
        c.feed(&snap("B", 1));
        let welcome = c.welcome_for(arb_core::model::Venue::Kalshi);
        assert_eq!(welcome.len(), 2);
        // Python is publishing C and the Rust recorder is not tracking it.
        let py: HashSet<String> = ["A", "B", "C"].iter().map(|s| s.to_string()).collect();
        assert_eq!(welcome_coverage_gap(&welcome, &py), vec!["C".to_string()]);
        // A market that is merely QUIET on the Rust tape is still in the
        // welcome, so it is not a hole. This is the false alarm being avoided.
        let py: HashSet<String> = ["A"].iter().map(|s| s.to_string()).collect();
        assert!(welcome_coverage_gap(&welcome, &py).is_empty());
        // and a snapshot from a DIFFERENT venue must not paper over a hole
        assert!(c.welcome_for(arb_core::model::Venue::PolymarketUs).is_empty());
    }

    #[test]
    fn a_stringified_money_object_on_the_wire_fails() {
        let mut c = StreamCheck::new();
        c.feed(&snap("A", 1));
        c.feed(
            r#"{"kind":"trade","venue":"polymarket_us","market_id":"A","price":"0.01","size":"{\"currency\":\"USD\",\"value\":\"4.0000\"}","taker_side":"sell","seq":1,"ts_local_ns":3,"ts_venue":null}"#,
        );
        let e = c.verdict().expect_err("a non-numeric size must fail");
        assert!(e.contains("not a number"), "{e}");
    }
}
