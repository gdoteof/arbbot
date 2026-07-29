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
//! Usage — from the repo root, with the Rust recorder running:
//!
//!   arb-shadow-gate [--day YYYY-MM-DD] [--py-dir data/raw] [--rs-dir data/raw-rs]
//!                   [--window-s 900] [--tail-bytes N] [--sample-s 60]
//!                   [--tolerance 0.01] [--socket data/arbbot-rs.sock]
//!                   [--recorder-exe rust/target/release/arb-recorder]
//!                   [--live-s 120] [--load 6] [--load-ceiling 40]
//!                   [--no-tape] [--no-live]
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

/// Returns, per venue, the markets the PYTHON recorder published in the window —
/// the live stage checks the Rust welcome burst against them.
fn tape_stage(a: &Args, fail: &mut bool) -> HashMap<&'static str, HashSet<String>> {
    let mut python_universe: HashMap<&'static str, HashSet<String>> = HashMap::new();
    let sample_ns = a.sample_s * 1_000_000_000;
    for venue in tape::VENUES {
        let py: PathBuf = a.py_dir.join(format!("{venue}-{}.jsonl", a.day));
        let rs: PathBuf = a.rs_dir.join(format!("{venue}-{}.jsonl", a.day));
        if !py.exists() && !rs.exists() {
            continue;
        }
        println!("== {venue}");
        if !py.exists() || !rs.exists() {
            let missing = if py.exists() { &rs } else { &py };
            println!("  FAIL: {} does not exist — nothing to compare", missing.display());
            *fail = true;
            continue;
        }
        // The window ENDS at the older of the two tapes' last events, so both
        // sides are asked about a span both of them could have covered.
        let (Ok(Some(py_end)), Ok(Some(rs_end))) = (tape::last_ts(&py), tape::last_ts(&rs)) else {
            println!("  FAIL: could not read a final timestamp from both tapes");
            *fail = true;
            continue;
        };
        let end = py_end.min(rs_end);
        let window = (end - a.window_s * 1_000_000_000, end);
        let (pys, pysam) = match tape::replay(&py, window, a.tail_bytes, sample_ns) {
            Ok(v) => v,
            Err(e) => {
                println!("  FAIL: reading {}: {e}", py.display());
                *fail = true;
                continue;
            }
        };
        let (rss, rssam) = match tape::replay(&rs, window, a.tail_bytes, sample_ns) {
            Ok(v) => v,
            Err(e) => {
                println!("  FAIL: reading {}: {e}", rs.display());
                *fail = true;
                continue;
            }
        };
        for (label, s) in [("python", &pys), ("rust", &rss)] {
            println!(
                "  {label:7} in_window={:>9} snap={:>8} delta={:>9} trade={:>7} markets={:>5} \
                 gaps={:>5} unsynced={:>6} undecodable={} bad_field={} covered={:.0}s",
                s.in_window,
                s.snapshot,
                s.delta,
                s.trade,
                s.markets.len(),
                s.gaps,
                s.unsynced,
                s.undecodable,
                s.bad_field,
                s.covered_s(),
            );
        }

        // --- GATING. Each of these three is a clause of M1's gate in
        // docs/migration-plan.md; everything below them is advisory, exactly as
        // shadow_gate.py had it ("volume/TOB numbers are judged per shadow mode").
        if rss.undecodable > 0 || rss.bad_field > 0 {
            println!(
                "  FAIL parse-compat: {} undecodable + {} non-numeric field(s) in the RUST tape",
                rss.undecodable, rss.bad_field
            );
            *fail = true;
        }
        if rss.gaps > pys.gaps {
            println!(
                "  FAIL gap counter: rust {} > python {} over the same window",
                rss.gaps, pys.gaps
            );
            *fail = true;
        }

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
        let pct = if total > 0 { 100.0 * agree as f64 / total as f64 } else { 0.0 };
        println!("  TOB agreement (tol {}): {agree}/{total} = {pct:.1}% [advisory]", a.tolerance);
        for (d, mkt, bucket) in worst {
            println!("    worst: {mkt} bucket={bucket} diff={d:.4}");
        }
        // A covered span that differs a lot between the sides means the byte
        // bound, not the recorders, decided what was compared.
        let (pc, rc) = (pys.covered_s(), rss.covered_s());
        if pc > 0.0 && rc > 0.0 && (pc - rc).abs() > 0.1 * pc.max(rc) {
            println!(
                "  note: covered spans differ ({pc:.0}s vs {rc:.0}s) — raise --tail-bytes before \
                 reading anything into the volume numbers"
            );
        }
        python_universe.insert(venue, pys.markets);
    }
    python_universe
}

fn live_stage(a: &Args, python_universe: &HashMap<&'static str, HashSet<String>>, fail: &mut bool) {
    println!("== live subscriber under load: {}", a.socket.display());
    if let Some(why) = socket_refusal(&a.socket) {
        println!("  FAIL: {why}");
        *fail = true;
        return;
    }
    let load = live::load1();
    let workers = match live::plan_workers(a.load, load, a.load_ceiling) {
        Ok(w) => w,
        Err(e) => {
            println!("  FAIL: {e}");
            *fail = true;
            return;
        }
    };
    println!("  load1={load:.2} workers={workers} (cap {}) for {}s", live::MAX_WORKERS, a.live_s);
    let r = match live::attach(&a.socket, a.live_s, workers, a.load_ceiling) {
        Ok(r) => r,
        Err(e) => {
            println!("  FAIL: attaching to {}: {e}", a.socket.display());
            *fail = true;
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
        Err(e) => {
            println!("  FAIL: {e}");
            *fail = true;
        }
    }
    // The universe gate. `python_universe` is empty when the tape stage did not
    // run, and an empty expectation is not a check — say so rather than pass.
    if python_universe.is_empty() {
        println!("  note: no python universe to check the welcome burst against (tape stage \
                  did not run), so COVERAGE WAS NOT CHECKED.");
    }
    for (venue, py_markets) in python_universe {
        let Some(v) = arb_core::model::Venue::parse(venue) else { continue };
        let welcome = c.welcome_for(v);
        let gap = live::welcome_coverage_gap(&welcome, py_markets);
        if gap.is_empty() {
            println!("  coverage {venue}: welcome has all {} python markets", py_markets.len());
        } else {
            println!(
                "  FAIL coverage {venue}: {} market(s) python published in the window are NOT \
                 in the rust recorder's welcome burst, e.g. {:?}",
                gap.len(),
                &gap[..gap.len().min(5)]
            );
            *fail = true;
        }
    }
    if r.workers == 0 {
        println!("  note: ZERO load workers ran, so this was not a contention test.");
    }

    // Everything above describes the tree. This describes the PROCESS.
    let images = live::running_images(&a.recorder_exe);
    if images.is_empty() {
        println!(
            "  FAIL: no running process has {} as its image, so NOTHING above describes a \
             running recorder. Pass --recorder-exe if it lives elsewhere.",
            a.recorder_exe.display()
        );
        *fail = true;
    }
    for img in images {
        match live::running_image_verdict(&img) {
            Ok(()) => println!("  image: {img} — current"),
            Err(e) => {
                println!("  FAIL: {e}");
                *fail = true;
            }
        }
    }
}

fn main() {
    let a = parse_args();
    let mut fail = false;
    println!(
        "### arb-shadow-gate day={} window={}s socket={}",
        a.day,
        a.window_s,
        a.socket.display()
    );
    let python_universe = if a.do_tape {
        tape_stage(&a, &mut fail)
    } else {
        println!("== tape stage: NOT RUN (--no-tape)");
        fail = true;
        HashMap::new()
    };
    if a.do_live {
        live_stage(&a, &python_universe, &mut fail);
    } else {
        println!("== live stage: NOT RUN (--no-live)");
        fail = true;
    }
    // A stage that did not run is not a stage that passed. `--no-tape` and
    // `--no-live` exist for iterating on one half; neither can produce a PASS,
    // for the same reason `gate.sh` refuses to say PASS after `--skip-digest`.
    println!("SHADOW GATE: {}", if fail { "FAIL" } else { "PASS" });
    if fail {
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
}
