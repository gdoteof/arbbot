//! arb-shadow-gate — the M1 recorder-cutover gate, in Rust.
//!
//! It replaces `arbbot-shadow-gate.{service,timer}`, which were deleted from
//! main in `c382ea5` because they invoked a worktree that no longer exists. The
//! last report they produced with a RESULT line in it is
//! `data/reports/shadow-gate-2026-07-23.txt`; every run after that is the
//! 153-byte `can't open file .../rust-rewrite/scripts/shadow_gate.py`. The
//! Python script survives and is readable, but Python is frozen and is not run.
//!
//! TWO STAGES, AND THE SECOND IS THE POINT.
//!
//!  1. TAPE — the `scripts/shadow_gate.py` comparison over a bounded window of
//!     both recorders' tapes. See `tape.rs`.
//!  2. LIVE — a real subscriber on the Rust recorder's socket, reading it under
//!     CPU contention. See `live.rs`.
//!
//! Stage 2 exists because stage 1 is not sufficient and was never going to be.
//! The recorder hang of 2026-07-29 (task #48) showed up only under CPU
//! contention, roughly one run in ten; a gate that diffs finished files would
//! have been green through all three sightings, because a tape says nothing
//! about whether the process writing it is still serving anybody. And nothing
//! HAS ever consumed the Rust recorder: `data/arbbot-rs.sock` has carried LISTEN
//! and zero peers since it was built, so the welcome burst, the fan-out and the
//! eviction path have run in production for weeks with no reader at all.
//!
//! WHAT THIS BINARY DOES NOT CHECK — say it here, because a gate that is
//! believed to cover more than it does is worse than no gate:
//!
//!  * **"7 consecutive green shadow-gate days"** (`docs/migration-plan.md`, M1)
//!    is NOT checked anywhere in here. This binary judges ONE day and knows
//!    nothing about any other run. Consecutiveness is a manual grep over
//!    `data/reports/`, in §1 of `docs/recorder-cutover-runbook.md`, and that
//!    grep is the only thing standing behind that clause.
//!  * **"parse-check PASS on every day in the window"** is under-covered. This
//!    gate decodes a byte-bounded TRAILING SLICE of the CURRENT day — roughly
//!    1% of a 6 GB tape — and never invokes `arb-recorder --parse-check`, which
//!    is the thing that reads a whole file. Each venue's line prints the slice
//!    size against the file size so the shortfall is on the report rather than
//!    in a reader's head.
//!  * Three of the checks here — welcome coverage, `bad_field`, and the
//!    running-image verdict — are ADDITIONS with no clause behind them. That is
//!    deliberate; what would be wrong is claiming every check is a clause.
//!
//! Usage — from the repo root, with the Rust recorder running:
//!
//!   arb-shadow-gate [--day YYYY-MM-DD] [--py-dir data/raw] [--rs-dir data/raw-rs]
//!                   [--window-s 900] [--tail-bytes N] [--sample-s 60]
//!                   [--tolerance 0.01] [--socket data/arbbot-rs.sock]
//!                   [--recorder-exe rust/target/release/arb-recorder]
//!                   [--live-s 120] [--load 6] [--load-ceiling 40]
//!                   [--no-tape] [--no-live]
//!
//! Run it under `nice -n 19`: the live stage deliberately burns CPU and the
//! ARMED engine's feed shares this box. The unit does that for you; a hand-run
//! does not, and this binary warns when its own niceness says nobody did.
//!
//! Exit 0 only on `SHADOW GATE: PASS`.

mod live;
mod tape;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

struct Args {
    day: String,
    py_dir: PathBuf,
    rs_dir: PathBuf,
    window_s: i64,
    tail_bytes: u64,
    sample_s: i64,
    tolerance: f64,
    socket: PathBuf,
    recorder_exe: PathBuf,
    live_s: u64,
    load: usize,
    load_ceiling: f64,
    do_tape: bool,
    do_live: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        day: arb_core::resolve::iso_from_day(arb_core::clock::now_secs() as i64 / 86_400),
        py_dir: "data/raw".into(),
        rs_dir: "data/raw-rs".into(),
        window_s: 900,
        // 256 MiB covers 900s of the busiest venue with room to spare
        // (PM-US ran ~72 KB/s on 2026-07-29) and bounds a 6 GB tape.
        tail_bytes: 268_435_456,
        sample_s: 60,
        tolerance: 0.01,
        socket: "data/arbbot-rs.sock".into(),
        recorder_exe: std::fs::canonicalize("rust/target/release/arb-recorder")
            .unwrap_or_else(|_| "rust/target/release/arb-recorder".into()),
        live_s: 120,
        load: 6,
        load_ceiling: live::DEFAULT_LOAD_CEILING,
        do_tape: true,
        do_live: true,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--day" => a.day = next(),
            "--py-dir" => a.py_dir = next().into(),
            "--rs-dir" => a.rs_dir = next().into(),
            "--window-s" => a.window_s = next().parse().expect("int"),
            "--tail-bytes" => a.tail_bytes = next().parse().expect("int"),
            "--sample-s" => a.sample_s = next().parse().expect("int"),
            "--tolerance" => a.tolerance = next().parse().expect("float"),
            "--socket" => a.socket = next().into(),
            "--recorder-exe" => {
                let p: PathBuf = next().into();
                a.recorder_exe = std::fs::canonicalize(&p).unwrap_or(p);
            }
            "--live-s" => a.live_s = next().parse().expect("int"),
            "--load" => a.load = next().parse().expect("int"),
            "--load-ceiling" => a.load_ceiling = next().parse().expect("float"),
            "--no-tape" => a.do_tape = false,
            "--no-live" => a.do_live = false,
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    a
}

/// Record one reason the gate is red, printing it where it was found.
///
/// A `Vec<String>` rather than a `bool` because the stage-level tests assert on
/// the REASON. `SHADOW GATE: FAIL` for an unrelated reason is indistinguishable
/// from the check under test working, and a suite that cannot tell those apart
/// is how a gate that passes on no input ships.
fn fail(fails: &mut Vec<String>, why: String) {
    println!("  FAIL: {why}");
    fails.push(why);
}

/// The live socket the ARMED engine is subscribed to. Attaching here would be
/// harmless in itself — the broadcaster fans out — but a gate that generates
/// CPU load has no business sharing a fate with the trader's feed, and a typo
/// is exactly how that happens.
const ARMED_SOCKET_NAME: &str = "arbbot.sock";

/// Why this socket may not be attached to, or `None` if it may be.
///
/// A function, and tested, because it is the only thing standing between a
/// mistyped `--socket` and a CPU-load generator sharing a fate with the armed
/// engine's market-data feed.
fn socket_refusal(socket: &Path) -> Option<String> {
    if socket.file_name().map(|n| n == ARMED_SOCKET_NAME).unwrap_or(false) {
        return Some(format!(
            "{ARMED_SOCKET_NAME} is the socket the ARMED engine is subscribed to. This stage \
             generates CPU load; point it at the Rust recorder's socket."
        ));
    }
    if !socket.exists() {
        return Some(format!("{} does not exist — is the Rust recorder up?", socket.display()));
    }
    None
}

/// `slice/total` MiB for one tape, so the report says how much of the day the
/// parse-compat verdict actually covers.
fn slice_mib(path: &Path, tail_bytes: u64) -> (u64, u64) {
    let total = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    (total.min(tail_bytes) / 1_048_576, total / 1_048_576)
}

/// Returns, per venue, the markets the PYTHON recorder published in the window —
/// the live stage checks the Rust welcome burst against them.
fn tape_stage(a: &Args, fails: &mut Vec<String>) -> HashMap<&'static str, HashSet<String>> {
    let mut python_universe: HashMap<&'static str, HashSet<String>> = HashMap::new();
    let sample_ns = a.sample_s * 1_000_000_000;
    for venue in tape::VENUES {
        let py: PathBuf = a.py_dir.join(format!("{venue}-{}.jsonl", a.day));
        let rs: PathBuf = a.rs_dir.join(format!("{venue}-{}.jsonl", a.day));
        println!("== {venue}");
        // BOTH TAPES MISSING IS A FAILURE, NOT A SKIP. It used to `continue`
        // before this venue had printed anything, so a mistyped `--day`,
        // `--py-dir` or `--rs-dir` skipped all three venues, produced no
        // output, and exited 0 on `SHADOW GATE: PASS` — the old gate's exact
        // failure mode, rebuilt in Rust. A venue that neither recorder wrote is
        // a venue nobody can certify.
        if !py.exists() || !rs.exists() {
            let missing: Vec<String> = [&py, &rs]
                .iter()
                .filter(|p| !p.exists())
                .map(|p| p.display().to_string())
                .collect();
            fail(
                fails,
                format!("{} does not exist — nothing to compare for {venue}", missing.join(" and ")),
            );
            continue;
        }
        // The window ENDS at the older of the two tapes' last events, so both
        // sides are asked about a span both of them could have covered.
        let (Ok(Some(py_end)), Ok(Some(rs_end))) = (tape::last_ts(&py), tape::last_ts(&rs)) else {
            fail(fails, format!("could not read a final timestamp from both {venue} tapes"));
            continue;
        };
        let end = py_end.min(rs_end);
        let window = (end - a.window_s * 1_000_000_000, end);
        let (pys, pysam) = match tape::replay(&py, window, a.tail_bytes, sample_ns) {
            Ok(v) => v,
            Err(e) => {
                fail(fails, format!("reading {}: {e}", py.display()));
                continue;
            }
        };
        let (rss, rssam) = match tape::replay(&rs, window, a.tail_bytes, sample_ns) {
            Ok(v) => v,
            Err(e) => {
                fail(fails, format!("reading {}: {e}", rs.display()));
                continue;
            }
        };
        for (label, s) in [("python", &pys), ("rust", &rss)] {
            println!(
                "  {label:7} in_window={:>9} snap={:>8} delta={:>9} trade={:>7} markets={:>5} \
                 gaps={:>5} unsynced={:>6} slice_lines={:>9} undecodable={} bad_field={} \
                 covered={:.0}s",
                s.in_window,
                s.snapshot,
                s.delta,
                s.trade,
                s.markets.len(),
                s.gaps,
                s.unsynced,
                s.lines,
                s.undecodable,
                s.bad_field,
                s.covered_s(),
            );
        }
        let (pslice, ptotal) = slice_mib(&py, a.tail_bytes);
        let (rslice, rtotal) = slice_mib(&rs, a.tail_bytes);
        println!(
            "  slice: python {pslice}/{ptotal} MiB, rust {rslice}/{rtotal} MiB — parse-compat \
             below is judged on THAT SLICE, not on the whole day"
        );

        // NOTHING COMPARED IS NOT A COMPARISON THAT PASSED. Every counter below
        // is zero when one side has no events in the window, which makes every
        // check after it vacuously green. This is not hypothetical: the window
        // ends at `min(py_end, rs_end)`, so a STALLED Python recorder — frozen
        // software the migration plan intends to stop — drags `end` hours into
        // the past while the byte-bounded Rust tail slice only reaches back
        // ~75 minutes at PM-US's measured rate. The two spans then do not
        // overlap at all and the arithmetic below reads `0 > 0`.
        if pys.in_window == 0 || rss.in_window == 0 {
            fail(
                fails,
                format!(
                    "nothing was compared for {venue}: python has {} and rust has {} event(s) in \
                     the {}s window ending {end}. Every check below is vacuous on an empty \
                     window. Raise --tail-bytes, or find out which recorder stalled.",
                    pys.in_window, rss.in_window, a.window_s
                ),
            );
            continue;
        }

        // --- GATING.
        if rss.undecodable > 0 || rss.bad_field > 0 {
            fail(
                fails,
                format!(
                    "parse-compat: {} undecodable + {} non-numeric field(s) in the RUST tape slice",
                    rss.undecodable, rss.bad_field
                ),
            );
        }
        // GAPS ARE GATED AGAINST ZERO, NOT AGAINST PYTHON'S COUNT.
        //
        // The comparison this used to make — `rust > python` — could not fail,
        // on any venue, ever, and the "gaps rust 0 vs python 63195" figures it
        // produced were a units error rather than evidence. The Rust recorder
        // assigns its OWN per-market sequence (`SeqCounter::next`, +1 each
        // event, applied on all three venues by construction); `BookBuilder`
        // raises `GapDetected` only when a delta's seq is not `book.seq + 1`,
        // which a +1 counter cannot produce. Python's Kalshi tape carries the
        // RAW WIRE sid sequence, which is per-subscription and therefore looks
        // like thousands of holes when replayed per-market. The two sides were
        // not measuring the same quantity, so `rust > python` was `0 > N`.
        //
        // What `rust > 0` means is sharp precisely BECAUSE the sequence is
        // synthetic: every event the recorder numbered n+1 was numbered while
        // holding the same lock it writes under, so a hole between two applied
        // events in the file means a line the recorder numbered never reached
        // the tape — a failed write, a truncation, or an undecodable line in
        // the middle of the slice. That is a real, reachable loss, and it is
        // the only gap question a tape can answer.
        //
        // The recorder's own `[hb] gaps=` counter is a DIFFERENT number again
        // (ingest-side `NotSynced` + `GapDetected` at the venue boundary, e.g.
        // 107 eleven minutes after a restart). It is not read here because it
        // exists only in the journal — it is not in `health-rs.jsonl` — and
        // reading it would couple this gate to a unit name. §1 of the runbook
        // reads it by hand instead, and says what it should look like.
        if rss.gaps > 0 {
            fail(
                fails,
                format!(
                    "{} sequence hole(s) in the RUST tape slice: the recorder numbers its own \
                     events +1 per market, so a hole is a numbered event that never reached the \
                     file",
                    rss.gaps
                ),
            );
        }
        println!(
            "  note: python gaps={} is printed, NOT compared — the two recorders number their \
             events differently (python's kalshi tape carries the raw per-subscription wire seq, \
             replayed per market), so this counts renumbering, not loss [advisory]",
            pys.gaps
        );

        // --- ADVISORY. The universe difference between the two TAPES is one of
        // these and not a gate: the Rust recorder dedups consecutive-identical
        // snapshots, so a market whose book has not moved inside the window is
        // absent from its tape and present in Python's, with nothing lost. The
        // gating version of this question is asked of the WELCOME BURST in the
        // live stage, where a missing market means the recorder does not have
        // the book at all.
        let missing = tape::missing_from_rs(&pys, &rss);
        if !missing.is_empty() {
            println!(
                "  note: {} market(s) in the python window and not the rust one (dedup, or a \
                 hole — the live stage decides), e.g. {:?}",
                missing.len(),
                &missing[..missing.len().min(5)]
            );
        }
        let extra = tape::missing_from_rs(&rss, &pys).len();
        if extra > 0 {
            println!("  note: {extra} market(s) only the rust tape saw (the superset claim)");
        }
        let (agree, differ, worst) = tape::tob_agreement(&pysam, &rssam, a.tolerance);
        let total = agree + differ;
        // "0/0 = 0.0%" reads as total disagreement and means the opposite: no
        // market was quoted on both sides in the same bucket, so nothing was
        // compared. It happens whenever a slice holds no snapshot for a venue
        // — every book is then unsynced and none of them form. Advisory either
        // way, but a vacuous measurement must not be printed as a result.
        if total == 0 {
            println!(
                "  TOB agreement (tol {}): n/a — NO market was sampled on both sides [advisory]",
                a.tolerance
            );
        } else {
            let pct = 100.0 * agree as f64 / total as f64;
            println!(
                "  TOB agreement (tol {}): {agree}/{total} = {pct:.1}% [advisory]",
                a.tolerance
            );
        }
        for (d, mkt, bucket) in worst {
            println!("    worst: {mkt} bucket={bucket} diff={d:.4}");
        }
        // A covered span that differs a lot between the sides means the byte
        // bound, not the recorders, decided what was compared. Both sides are
        // non-zero here — the empty-window case failed above rather than
        // reaching this line, which is where it used to be silently skipped.
        let (pc, rc) = (pys.covered_s(), rss.covered_s());
        if (pc - rc).abs() > 0.1 * pc.max(rc) {
            println!(
                "  note: covered spans differ ({pc:.0}s vs {rc:.0}s) — raise --tail-bytes before \
                 reading anything into the volume numbers"
            );
        }
        python_universe.insert(venue, pys.markets);
    }
    python_universe
}

fn live_stage(
    a: &Args,
    python_universe: &HashMap<&'static str, HashSet<String>>,
    fails: &mut Vec<String>,
) {
    println!("== live subscriber under load: {}", a.socket.display());
    // THE EXPECTATION IS CHECKED BEFORE THE SOCKET, because an expectation that
    // is EMPTY is not an expectation that was met. `welcome_coverage_gap` is a
    // set difference: against an empty python set it returns nothing and the
    // gate printed "welcome has all 0 python markets" and passed. The
    // `is_empty()` below used to be a `note:` on the MAP, which never looked
    // inside the sets at all.
    if python_universe.is_empty() {
        fail(
            fails,
            "COVERAGE WAS NOT CHECKED: no python universe to check the welcome burst against. \
             The tape stage did not run, or produced nothing for any venue."
                .to_owned(),
        );
    }
    for (venue, py_markets) in python_universe {
        if py_markets.is_empty() {
            fail(
                fails,
                format!(
                    "COVERAGE WAS NOT CHECKED for {venue}: the python window held ZERO markets, \
                     so the welcome burst would be compared against nothing"
                ),
            );
        }
    }
    if let Some(why) = socket_refusal(&a.socket) {
        fail(fails, why);
        return;
    }
    let load = live::load1();
    let workers = match live::plan_workers(a.load, load, a.load_ceiling) {
        Ok(w) => w,
        Err(e) => {
            fail(fails, e);
            return;
        }
    };
    println!(
        "  load1={load:.2} workers={workers} (cap {}) for {}s at nice={}",
        live::MAX_WORKERS,
        a.live_s,
        live::own_nice().map_or("?".to_owned(), |n| n.to_string())
    );
    // `Nice=19` is a property of the UNIT, not of this binary, and the runbook's
    // own pre-flight invokes it by hand. Six unniced burners on the box carrying
    // the armed engine's feed outrank the Rust recorder (measured NI 10) — that
    // has happened, and it degraded the live feed.
    if workers > 0 && live::own_nice().is_some_and(|n| n < 10) {
        println!(
            "  note: this gate is NOT niced and is about to burn {workers} cores next to the \
             armed engine's feed. Re-run it as `nice -n 19 rust/target/release/arb-shadow-gate ...`"
        );
    }
    let r = match live::attach(&a.socket, a.live_s, workers, a.load_ceiling) {
        Ok(r) => r,
        Err(e) => {
            fail(fails, format!("attaching to {}: {e}", a.socket.display()));
            return;
        }
    };
    let c = &r.check;
    println!(
        "  read {} lines in {:.0}s: welcome={} markets, snap={} delta={} trade={} \
         markets={} gaps={} unsynced={} undecodable={} bad_field={}",
        c.lines,
        r.elapsed_s,
        c.welcome_markets,
        c.snapshots,
        c.deltas,
        c.trades,
        c.markets.len(),
        c.gaps,
        c.unsynced,
        c.undecodable,
        c.bad_field,
    );
    println!(
        "  load1 {:.2} -> peak {:.2} (ceiling {:.0}){}",
        r.load_start,
        r.load_peak,
        a.load_ceiling,
        if r.load_aborted { " *** WORKERS STOPPED EARLY: ceiling crossed ***" } else { "" }
    );
    match c.verdict() {
        Ok(()) => println!("  live: ok"),
        Err(e) => fail(fails, e),
    }
    for (venue, py_markets) in python_universe {
        if py_markets.is_empty() {
            continue; // already failed above; a set difference against it says nothing
        }
        let Some(v) = arb_core::model::Venue::parse(venue) else { continue };
        let welcome = c.welcome_for(v);
        let gap = live::welcome_coverage_gap(&welcome, py_markets);
        if gap.is_empty() {
            println!("  coverage {venue}: welcome has all {} python markets", py_markets.len());
        } else {
            fail(
                fails,
                format!(
                    "coverage {venue}: {} market(s) python published in the window are NOT in the \
                     rust recorder's welcome burst, e.g. {:?}",
                    gap.len(),
                    &gap[..gap.len().min(5)]
                ),
            );
        }
    }
    if r.workers == 0 {
        println!("  note: ZERO load workers ran, so this was not a contention test.");
    }

    // Everything above describes the tree. This describes the PROCESS.
    let images = live::running_images(&a.recorder_exe);
    if images.is_empty() {
        fail(
            fails,
            format!(
                "no running process has {} as its image, so NOTHING above describes a running \
                 recorder. Pass --recorder-exe if it lives elsewhere.",
                a.recorder_exe.display()
            ),
        );
    }
    for img in images {
        match live::running_image_verdict(&img) {
            Ok(()) => println!("  image: {img} — current"),
            Err(e) => fail(fails, e),
        }
    }
}

/// Every reason the gate is red. Empty is a PASS.
///
/// Separated from `main` so the PASS/FAIL decision itself is testable. It was
/// not, and the consequence was a gate that exited 0 with no output at all on a
/// wrong `--day`: fourteen tests, every one of them on a leaf.
fn run(a: &Args) -> Vec<String> {
    let mut fails: Vec<String> = Vec::new();
    println!(
        "### arb-shadow-gate day={} window={}s socket={}",
        a.day,
        a.window_s,
        a.socket.display()
    );
    // THE VERDICT IS STAKED OUT BEFORE ANY WORK IS DONE. Rust's stdout is
    // line-buffered even into a pipe, so this line reaches the `tee`'d report
    // immediately. If it is the LAST `SHADOW GATE:` line in a report, the run
    // was killed before it decided — systemd's `TimeoutStartSec`, an OOM kill,
    // a Ctrl-C — and the file is a truncated one, not a passing one.
    //
    // Without it a killed run leaves a report with NO verdict line, which is
    // byte-for-byte the operator signal the three 153-byte `can't open file`
    // reports gave, and the soak grep in the runbook prints nothing for it —
    // indistinguishable from "the gate was never installed". That silence is
    // the defect this whole PR exists to fix; it must not be reachable from
    // inside the fix.
    println!("SHADOW GATE: NO VERDICT — this run has not finished");
    let python_universe = if a.do_tape {
        tape_stage(a, &mut fails)
    } else {
        println!("== tape stage");
        fail(&mut fails, "tape stage NOT RUN (--no-tape)".to_owned());
        HashMap::new()
    };
    if a.do_live {
        live_stage(a, &python_universe, &mut fails);
    } else {
        println!("== live stage");
        fail(&mut fails, "live stage NOT RUN (--no-live)".to_owned());
    }
    // A stage that did not run is not a stage that passed. `--no-tape` and
    // `--no-live` exist for iterating on one half; neither can produce a PASS,
    // for the same reason `gate.sh` refuses to say PASS after `--skip-digest`.
    if !fails.is_empty() {
        println!("--- {} reason(s) this gate is red:", fails.len());
        for f in &fails {
            println!("  * {f}");
        }
    }
    println!("SHADOW GATE: {}", if fails.is_empty() { "PASS" } else { "FAIL" });
    fails
}

fn main() {
    if !run(&parse_args()).is_empty() {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one path in this binary that can touch the armed engine's feed.
    #[test]
    fn the_live_stage_refuses_the_armed_engines_socket() {
        let why = socket_refusal(Path::new("/home/geoff/claude/arbbot/data/arbbot.sock"))
            .expect("the armed engine's socket must be refused");
        assert!(why.contains("ARMED engine"), "{why}");
        // and it is refused by NAME, not by absence: a path that does not exist
        // must give the other reason, or the guard is untested where it counts.
        let why = socket_refusal(Path::new("/nonexistent/arbbot.sock")).expect("refused");
        assert!(why.contains("ARMED engine"), "{why}");
        let why = socket_refusal(Path::new("/nonexistent/arbbot-rs.sock")).expect("refused");
        assert!(why.contains("does not exist"), "{why}");
    }

    // --- STAGE-LEVEL. Everything above this line, and every test in `tape.rs`
    // and `live.rs`, is a leaf. A suite made only of leaves cannot see a stage
    // that never called them: `tape_stage` skipping all three venues and
    // returning an empty map turned NOTHING red, and that is exactly the bug
    // that shipped. These tests drive the stage functions and `run` itself.

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("arb-sgate-main-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(d.join("py")).expect("tmpdir");
        std::fs::create_dir_all(d.join("rs")).expect("tmpdir");
        d
    }

    /// `--live-s 0`, `--load 0` and a socket that does not exist: no test here
    /// attaches to anything or burns a core.
    fn test_args(dir: &Path, day: &str) -> Args {
        Args {
            day: day.to_owned(),
            py_dir: dir.join("py"),
            rs_dir: dir.join("rs"),
            window_s: 900,
            tail_bytes: u64::MAX,
            sample_s: 60,
            tolerance: 0.01,
            socket: dir.join("no-such-recorder.sock"),
            recorder_exe: "/no/such/binary".into(),
            live_s: 0,
            load: 0,
            load_ceiling: live::DEFAULT_LOAD_CEILING,
            do_tape: true,
            do_live: false,
        }
    }

    fn snap(venue: &str, mkt: &str, seq: u64, ts: i64) -> String {
        format!(
            r#"{{"kind":"snapshot","venue":"{venue}","market_id":"{mkt}","bids":[{{"price":"0.40","size":"10"}}],"asks":[{{"price":"0.60","size":"10"}}],"seq":{seq},"ts_local_ns":{ts},"ts_venue":null}}"#
        )
    }
    fn delta(venue: &str, mkt: &str, seq: u64, ts: i64) -> String {
        format!(
            r#"{{"kind":"delta","venue":"{venue}","market_id":"{mkt}","side":"bid","price":"0.41","size":"5","seq":{seq},"ts_local_ns":{ts},"ts_venue":null}}"#
        )
    }
    fn tape(a: &Args, side: &str, venue: &str, lines: &[String]) {
        let dir = if side == "py" { &a.py_dir } else { &a.rs_dir };
        std::fs::write(
            dir.join(format!("{venue}-{}.jsonl", a.day)),
            format!("{}\n", lines.join("\n")),
        )
        .expect("write tape");
    }

    /// A clean, matching pair for every venue. The tests that isolate one
    /// failure overwrite one venue on top of this, so the assertion can be on
    /// the failure COUNT and not just on a substring.
    fn all_venues_clean(a: &Args) {
        for venue in tape::VENUES {
            let lines = [
                snap(venue, "A", 1, 1_000_000_000),
                delta(venue, "A", 2, 2_000_000_000),
                snap(venue, "B", 1, 3_000_000_000),
            ];
            tape(a, "py", venue, &lines);
            tape(a, "rs", venue, &lines);
        }
    }

    /// BLOCKING 1, and the reason this file has stage-level tests at all.
    ///
    /// A wrong `--day` (or `--py-dir`, or `--rs-dir`) used to hit the
    /// both-tapes-missing `continue` before the venue header printed, for all
    /// three venues, and the binary then said `SHADOW GATE: PASS` and exited 0
    /// having compared nothing and printed almost nothing. That is the failure
    /// mode of the gate this one replaces, reproduced inside the replacement.
    #[test]
    fn a_day_with_no_tapes_at_all_is_a_failure_not_a_pass() {
        let dir = tmpdir("noday");
        let a = test_args(&dir, "1999-01-01");
        let fails = run(&a);
        assert!(
            fails.iter().any(|f| f.contains("nothing to compare for kalshi")),
            "the missing kalshi tape must be a REASON, not a skip: {fails:?}"
        );
        assert_eq!(
            fails.iter().filter(|f| f.contains("nothing to compare")).count(),
            tape::VENUES.len(),
            "every venue must be named, not just the first: {fails:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// The positive control. Without it, "make `tape_stage` return an empty
    /// map" is a mutation no test can see.
    #[test]
    fn two_matching_tapes_produce_no_failures_and_a_python_universe() {
        let dir = tmpdir("match");
        let a = test_args(&dir, "2020-01-02");
        all_venues_clean(&a);
        let mut fails = Vec::new();
        let universe = tape_stage(&a, &mut fails);
        assert_eq!(fails, Vec::<String>::new(), "matching tapes must not fail");
        assert_eq!(universe.len(), tape::VENUES.len(), "every venue reports a universe");
        let kalshi = universe.get("kalshi").expect("the kalshi universe must be reported");
        assert_eq!(kalshi.len(), 2, "both markets: {kalshi:?}");
        std::fs::remove_dir_all(dir).ok();
    }

    /// BLOCKING 1, second path. The window ends at `min(py_end, rs_end)`, so a
    /// STALLED recorder on either side leaves the other side's byte-bounded
    /// tail slice entirely outside it. Every counter is then zero, every check
    /// reads `0 > 0`, and the gate passes having compared nothing.
    #[test]
    fn a_window_that_compares_zero_events_is_a_failure() {
        let dir = tmpdir("emptywindow");
        let a = test_args(&dir, "2020-01-03");
        all_venues_clean(&a);
        // python's kalshi tape ends long before the rust one begins: `end`
        // lands on python's last event and the whole rust slice is in the
        // future. Every counter is then zero on the rust side.
        tape(&a, "py", "kalshi", &[snap("kalshi", "A", 1, 1_000_000_000)]);
        tape(
            &a,
            "rs",
            "kalshi",
            &[
                snap("kalshi", "A", 1, 9_000_000_000_000_000),
                delta("kalshi", "A", 2, 9_000_000_000_000_001),
            ],
        );
        let mut fails = Vec::new();
        let universe = tape_stage(&a, &mut fails);
        assert_eq!(fails.len(), 1, "exactly the empty window, nothing else: {fails:?}");
        assert!(
            fails[0].contains("nothing was compared for kalshi"),
            "zero events on a side must be a failure: {fails:?}"
        );
        assert!(
            !universe.contains_key("kalshi"),
            "a venue that compared nothing must not seed the coverage check"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// BLOCKING 3. The old check was `rust.gaps > python.gaps`, and this tape
    /// pair passes it — python has TWO holes and rust has one — while the rust
    /// tape has demonstrably lost a line. The Rust recorder numbers its own
    /// events +1 per market, so `rust > 0` is the only form of this check that
    /// can ever fail.
    #[test]
    fn one_hole_in_the_rust_tape_fails_even_when_python_has_more() {
        let dir = tmpdir("gaps");
        let a = test_args(&dir, "2020-01-04");
        all_venues_clean(&a);
        tape(
            &a,
            "py",
            "kalshi",
            &[
                snap("kalshi", "A", 1, 1_000_000_000),
                delta("kalshi", "A", 5, 2_000_000_000), // hole one
                snap("kalshi", "A", 6, 3_000_000_000),
                delta("kalshi", "A", 9, 4_000_000_000), // hole two
            ],
        );
        tape(
            &a,
            "rs",
            "kalshi",
            &[
                snap("kalshi", "A", 1, 1_000_000_000),
                delta("kalshi", "A", 2, 2_000_000_000),
                delta("kalshi", "A", 9, 3_000_000_000), // one hole
            ],
        );
        let mut fails = Vec::new();
        tape_stage(&a, &mut fails);
        assert_eq!(fails.len(), 1, "exactly the rust hole, nothing else: {fails:?}");
        assert!(
            fails[0].contains("sequence hole(s) in the RUST tape"),
            "a hole in the rust tape must fail regardless of python's count: {fails:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// BLOCKING 1, third path. `welcome_coverage_gap` is a set difference, so
    /// an EMPTY python set makes it return nothing and the gate printed
    /// "welcome has all 0 python markets" and passed. The guard used to test
    /// the MAP for emptiness and never looked inside the sets.
    #[test]
    fn an_empty_python_universe_is_a_coverage_check_that_did_not_happen() {
        let dir = tmpdir("vacuous");
        let mut a = test_args(&dir, "2020-01-05");
        a.do_live = true;

        let mut fails = Vec::new();
        live_stage(&a, &HashMap::new(), &mut fails);
        assert!(
            fails.iter().any(|f| f.contains("COVERAGE WAS NOT CHECKED")),
            "no universe at all must fail: {fails:?}"
        );

        // and the same for a venue that IS present carrying an empty set —
        // the case the map-level `is_empty()` could never see.
        let mut fails = Vec::new();
        let universe: HashMap<&'static str, HashSet<String>> =
            [("kalshi", HashSet::new())].into_iter().collect();
        live_stage(&a, &universe, &mut fails);
        assert!(
            fails.iter().any(|f| f.contains("COVERAGE WAS NOT CHECKED for kalshi")),
            "an empty market set must fail: {fails:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// `--no-tape`/`--no-live` were already refused; this pins the refusal to
    /// the verdict rather than to a comment about it.
    #[test]
    fn a_stage_that_did_not_run_is_not_a_stage_that_passed() {
        let dir = tmpdir("skipped");
        let mut a = test_args(&dir, "2020-01-06");
        a.do_tape = false;
        let fails = run(&a);
        assert!(fails.iter().any(|f| f.contains("tape stage NOT RUN")), "{fails:?}");
        assert!(fails.iter().any(|f| f.contains("live stage NOT RUN")), "{fails:?}");
        std::fs::remove_dir_all(dir).ok();
    }
}
