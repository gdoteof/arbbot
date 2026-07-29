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
//!    gate decodes a byte-bounded TRAILING SLICE of the CURRENT day — 256 MiB,
//!    which is 4-6% of the 3.8-6.6 GB these tapes reach — and never invokes
//!    `arb-recorder --parse-check`, which is the thing that reads a whole file.
//!    Measure that shortfall in BYTES, not in the 900s window: `tape.rs` counts
//!    `undecodable` and `bad_field` over the whole slice on purpose, so the
//!    SLICE is what parse-compat covers, and 900s/86400s = 1% understates it by
//!    4-6x. Each venue's line prints slice against file size so the number is
//!    on the report rather than in a reader's head.
//!  * **On `polymarket_us` the gap check cannot fire at all**, so the tape
//!    stage on that venue is parse-compat and nothing else.
//!    `pmus::parse_ws_message` emits only `Snapshot` and `Trade`;
//!    `BookBuilder::apply_event` inserts snapshots unconditionally and returns
//!    `Ok(())` for trades, so `GapDetected` has no reachable call path.
//!    Measured on the 2026-07-29 Rust tape: 6,582,352 snapshots, 0 deltas, and
//!    the same 0 deltas over the 256 MiB slice the gate actually reads. That is
//!    the venue carrying the most bytes of the three, and the one this PR names
//!    as corrupt.
//!  * **"Compared" here means BOTH TAPES HAD EVENTS, not that the two tapes
//!    were diffed.** Every GATING check in the tape stage reads the RUST tape
//!    only — `undecodable`, `bad_field`, `gaps`. The Python side gates on
//!    exactly one thing, that its window is non-empty; its market set is then
//!    handed to the live stage, where the only two-sided GATE in this binary
//!    lives (welcome coverage). Volume and TOB agreement are two-sided and
//!    advisory, as they were in the Python gate. Widening that is a design
//!    question, not a wording one.
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
        // 256 MiB covers 900s of the busiest venue with room to spare and
        // bounds a multi-GB tape. Measured on PM-US's own Rust tape: 48 KB/s
        // over the whole of 2026-07-28 (4,180,061,342 B / 86,400 s), 58 KB/s
        // over the trailing slice. 256 MiB at the trailing rate reaches back
        // 4,616 s = 77 min against a 900 s window — the same measurement as the
        // "~75 minutes" quoted at the empty-window check below, and the two
        // must move together.
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

/// `slice/total` BYTES for one tape, so the report says how much of the day the
/// parse-compat verdict actually covers.
///
/// Bytes, not MiB. It used to divide integer MiB and print `slice: python 0/0
/// MiB` for any tape under a megabyte, which reads identically to "nothing was
/// measured" — the same shape as the `0/0 = 0.0%` TOB line already fixed here,
/// and a quiet venue, an early-morning run and a venue that has just
/// reconnected all produce sub-MiB tapes. `human_bytes` keeps the unit
/// readable without a unit that can round a real file to zero.
fn slice_bytes(path: &Path, tail_bytes: u64) -> (u64, u64) {
    let total = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    (total.min(tail_bytes), total)
}

/// A byte count a human can read, exact below a kibibyte so nothing non-empty
/// can print as `0`.
fn human_bytes(n: u64) -> String {
    match n {
        n if n >= 1_048_576 => format!("{:.1} MiB", n as f64 / 1_048_576.0),
        n if n >= 1024 => format!("{:.1} KiB", n as f64 / 1024.0),
        n => format!("{n} B"),
    }
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
        let (pslice, ptotal) = slice_bytes(&py, a.tail_bytes);
        let (rslice, rtotal) = slice_bytes(&rs, a.tail_bytes);
        println!(
            "  slice: python {}/{}, rust {}/{} — parse-compat below is judged on THAT SLICE, \
             not on the whole day",
            human_bytes(pslice),
            human_bytes(ptotal),
            human_bytes(rslice),
            human_bytes(rtotal),
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
        // RETRACTED, and this is the second time this check's justification has
        // been wrong. It used to read: "every event the recorder numbered n+1
        // was numbered while holding the same lock it writes under, so a hole
        // means a line the recorder numbered never reached the tape". THAT IS
        // FALSE. `SeqCounter::next` (`arb-recorder/src/core.rs`) is a plain
        // `HashMap` bump in the VENUE TASK; the core mutex is taken later, on
        // the first line of `Core::on_event`. Nothing ties the two together,
        // and there are call sites that SPEND a number and then drop it —
        // named by symbol, not by line, because these moved twice while this
        // comment was being written:
        //
        //   pmintl::ws_task, integrity sweep  `let s = seq.next(tid);` then
        //     `clob.book(tid, s).await`, `Err(_) => resnap.failed += 1`
        //   pmintl::ws_task, gap recovery     the same shape on `seq.next(&need)`
        //   kalshi::resnap_slice              the same around `orderbook_fp(t)`
        //   kalshi::on_ws_message             `on_delta` returns None on a
        //     missing field with the number already spent
        //   pmus::poll_task                   `catalog.book(slug, seq.next(slug))`
        //
        // A spent-and-dropped number leaves the NEXT delta one ahead of what
        // `BookBuilder::apply_delta` expects, which is read as a gap and
        // DELETES the book. The repo already documents that exact consequence
        // in `pmintl::parse_ws_frame`, over the `{asset}|tape` trade counter.
        // So a hole here is "a numbered event that is not in the file", which
        // is a WEAKER claim than "a line was lost" — the failure text says so.
        //
        // The check still GATES, and the reason is measurement rather than
        // argument. Replaying the Rust tapes under this rule, in independent
        // 900s windows the way `tape::replay` sees them (book state starts
        // empty at the window edge, because the window filter runs before
        // `apply_event`):
        //
        //   kalshi   2026-07-28   0 holes    0 of 95 windows red
        //   kalshi   2026-07-29   0 holes    0 of 88 windows red
        //   pm-us    2026-07-29   0 holes    0 of 88 windows red (see above:
        //                                    it has no deltas, so it cannot)
        //   pm-intl  2026-07-28   4,747      92 of 95 windows red
        //   pm-intl  2026-07-29     800      18 of 88 windows red
        //
        // and the pm-intl reds are not spread across either day. On 07-29 they
        // are windows 0..18 and nothing after: the last red window ends 04:30
        // UTC, and `b1f990a` ("an INTL trade deleted its own market's book",
        // ticket #13) landed at 00:42 local with a recorder restart behind it.
        // The 69 windows after it — 17.25 consecutive hours, all three venues —
        // hold ZERO holes. 07-28 is that same bug over a whole day before the
        // fix. It burned a book sequence number on every trade, which is the
        // spend-and-drop shape above, and this check is what would have caught
        // it: 92 red windows against a real P1 that was deleting books.
        //
        // A resnapshot failure has NOT been observed to redden it, and the
        // arithmetic that says it should (`[pm-ws] resnapshot sweep: 3/111
        // books did NOT refresh` on most cycles, ~276 cycles/day, ~828 burns)
        // does not survive the clustering: 828 burns spread over a day cannot
        // produce 800 holes confined to its first four and a half hours. The
        // three tokens that fail the sweep are evidently tokens the CLOB will
        // not serve, and a token the CLOB will not serve sends no deltas to
        // trip over the hole.
        //
        // WHEN IT DOES GO RED, READ IT LIKE THIS, because a gate that goes red
        // for a benign reason nobody can diagnose is how the last one came to
        // be ignored: 98.8% of the holes above are exactly ONE missing number,
        // which is the signature of a single spend-and-drop. Check the venue's
        // resnapshot-failure line and `[hb] gaps=` FIRST. A hole wider than one
        // number, or one on a venue whose sweep is clean, is the tape-loss
        // reading. The recorder defect itself is filed separately; fixing it
        // means assigning the number after the `Ok`, and it is not this PR.
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
                     events +1 per market, so a hole is a numbered event that is NOT in the file. \
                     Two readings — a line was lost, or the recorder SPENT a number on a call that \
                     then failed (pmintl::ws_task, kalshi::resnap_slice, pmus::poll_task). Check \
                     the venue's \
                     resnapshot-failure line first; a hole wider than one number is the lost-line \
                     reading",
                    rss.gaps
                ),
            );
        }
        // Named per venue rather than as one sentence about kalshi. The old
        // wording printed "python's kalshi tape carries the raw wire seq" under
        // the `== polymarket` header, next to a non-zero python gap count it
        // did not explain — an advisory that misattributes its own number.
        println!(
            "  note: python gaps={} is printed, NOT compared — the two recorders number their \
             {venue} events differently, so this counts renumbering, not loss [advisory]",
            pys.gaps
        );

        // --- ADVISORY. The universe difference between the two TAPES is one of
        // these and not a gate, but NOT for the reason this comment used to
        // give. It said "the Rust recorder dedups consecutive-identical
        // snapshots". There is no dedup anywhere in the recorder:
        // `Core::on_event` writes every event it is handed, and
        // `JsonlWriter::write` appends unconditionally. Every emitter always
        // emits.
        //
        // The real asymmetry is POLLED vs EVENT-DRIVEN. Rust PM-US runs
        // `pmus::ws_task` when credentials exist (it does), so it emits only
        // when the venue pushes a frame; Python polls on an interval and emits
        // whether or not anything moved. Measured today: 19.4% of consecutive
        // Python PM-US lines are identical to their predecessor against 2.2% of
        // Rust's. So a market that has not traded inside the window is absent
        // from the Rust tape because nothing happened to it, not because a
        // duplicate was suppressed — same conclusion, different mechanism, and
        // gating on it would still cry wolf.
        //
        // The gating version of this question is asked of the WELCOME BURST in
        // the live stage, where a missing market means the recorder does not
        // have the book at all.
        let missing = tape::missing_from_rs(&pys, &rss);
        if !missing.is_empty() {
            println!(
                "  note: {} market(s) in the python window and not the rust one (python POLLS and \
                 rust is event-driven, so a quiet market is absent from rust — or it is a real \
                 hole; the live stage decides), e.g. {:?}",
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

/// The welcome burst against the python universe, one venue at a time.
///
/// Split out of `live_stage` so it is reachable from a test without a socket,
/// a recorder and 120 seconds. The `Venue::parse` arm below was the last silent
/// `continue` in the verdict path and there was no way to drive it.
fn coverage_checks(
    c: &live::StreamCheck,
    python_universe: &HashMap<&'static str, HashSet<String>>,
    fails: &mut Vec<String>,
) {
    for (venue, py_markets) in python_universe {
        if py_markets.is_empty() {
            continue; // already failed above; a set difference against it says nothing
        }
        // NOT a `continue`. Dead today — `tape::VENUES` and `Venue` agree on all
        // three — and the day a fourth venue is added to one and not the other,
        // a `continue` here drops that venue's coverage check with no line on
        // the report at all. Silently checking less than the report implies is
        // the failure mode this whole gate is a rewrite of.
        let Some(v) = arb_core::model::Venue::parse(venue) else {
            fail(
                fails,
                format!(
                    "coverage {venue}: NOT CHECKED — `Venue::parse` does not know this venue, so \
                     the welcome burst was never asked about it. `tape::VENUES` and \
                     `arb_core::model::Venue` have diverged."
                ),
            );
            continue;
        };
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
    coverage_checks(c, python_universe, fails);
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

/// Staked out before anything that can exit, and checked by
/// `tests/verdict_sentinel.rs`.
const NO_VERDICT: &str = "SHADOW GATE: NO VERDICT — this run has not finished";

fn main() {
    // THE VERDICT IS STAKED OUT BEFORE ANY WORK IS DONE — including
    // `parse_args()`, which is why this is the first statement in `main` and
    // not the first statement in `run`. Rust's stdout is line-buffered even
    // into a pipe (`std/src/io/stdio.rs` builds a `LineWriter`
    // unconditionally), so it reaches the `tee`'d report immediately.
    //
    // If it is the LAST `SHADOW GATE:` line in a report, the run was killed
    // before it decided — systemd's `TimeoutStartSec`, an OOM kill, a Ctrl-C —
    // and the file is a truncated one, not a passing one.
    //
    // Without it a killed run leaves a report with NO verdict line, which is
    // byte-for-byte the operator signal the three 153-byte `can't open file`
    // reports gave, and the soak grep in the runbook prints nothing for it —
    // indistinguishable from "the gate was never installed". That silence is
    // the defect this whole PR exists to fix; it must not be reachable from
    // inside the fix.
    //
    // It used to sit inside `run`, AFTER `parse_args`, and `parse_args` exits 2
    // on an unknown flag and panics on a flag missing its value. Since the unit
    // appends (`tee -a`, so a hand-run cannot erase the timer's report), either
    // exit left the PREVIOUS run's `PASS` standing as the last `SHADOW GATE:`
    // line in the day's file — a typo'd hand-run made a day look green.
    println!("{NO_VERDICT}");
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

    /// A venue in `tape::VENUES` that `arb_core::model::Venue` does not know
    /// used to `continue` — dropping that venue's coverage check with nothing
    /// on the report. Dead today; the day a fourth venue lands in one list and
    /// not the other it is a silent loss of coverage in the verdict path.
    #[test]
    fn a_venue_the_model_does_not_know_is_a_failure_not_a_skip() {
        let c = live::StreamCheck::new();
        let universe: HashMap<&'static str, HashSet<String>> =
            [("not_a_venue", ["M".to_owned()].into_iter().collect())].into_iter().collect();
        let mut fails = Vec::new();
        coverage_checks(&c, &universe, &mut fails);
        assert_eq!(fails.len(), 1, "{fails:?}");
        assert!(fails[0].contains("Venue::parse"), "{fails:?}");
        assert!(fails[0].contains("NOT CHECKED"), "{fails:?}");
    }

    /// A sub-megabyte tape printed `slice: python 0/0 MiB`, which reads as
    /// "nothing was measured" and is the same shape as the `0/0 = 0.0%` TOB
    /// line already fixed here. Only a genuinely empty file may print `0`.
    #[test]
    fn a_sub_megabyte_tape_does_not_report_itself_as_zero() {
        let dir = tmpdir("submib");
        let p = dir.join("small.jsonl");
        std::fs::write(&p, vec![b'x'; 4096]).expect("write");
        let (slice, total) = slice_bytes(&p, u64::MAX);
        assert_eq!((slice, total), (4096, 4096), "the whole file is inside the slice");
        assert_eq!(human_bytes(slice), "4.0 KiB", "a 4 KiB tape must not print as 0");
        // the tail bound still bites when the file is bigger than it
        let (slice, total) = slice_bytes(&p, 1024);
        assert_eq!((human_bytes(slice), human_bytes(total)), ("1.0 KiB".into(), "4.0 KiB".into()));
        // and the readable unit never rounds a non-empty file away
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1), "1 B");
        assert_eq!(human_bytes(1_048_576), "1.0 MiB");
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
