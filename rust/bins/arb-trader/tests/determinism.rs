//! Determinism gates for the P4 shell additions: fill ingestion and the
//! engine-sequenced WAL.
//!
//! The frozen-tape digest pin (3bf83fa1…, 80 intents) is the regression gate
//! for the real registry and lives in the ops runbook — it needs a 1.9 GB tape
//! and the private registry, so it cannot run here. These tests use a
//! self-contained synthetic tape (tests/fixtures/) that exercises what the
//! frozen tape cannot: it contains no fill events at all.
//!
//! What is proven here:
//!   1. the engine is a deterministic fold — two runs over the same tape emit
//!      byte-identical intents, hedge_needed lines included;
//!   2. WAL replay is engine-path-identical — replaying the WAL a run produced
//!      reproduces that run's intents and digest byte-for-byte;
//!   3. --wal is decision-neutral — logging changes no output;
//!   4. the fill semantics that reach the intent stream: cumulative
//!      idempotence, fill-racing-cancel, foreign order ids, and quote-time
//!      hedge anchoring (the anchor survives a hedge-leg book move).

use std::path::{Path, PathBuf};
use std::process::Command;

fn data(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

struct Run {
    digest: String,
    summary: serde_json::Value,
    intents: String,
    /// The engine's operator-facing log. The ledger-booking and unattributed-fill
    /// lines are only observable here: a bench run must never write the real
    /// trade ledger, so what it BOOKED can only be read off stderr.
    stderr: String,
}

/// Run the shell over `source` (`--bench-tape` or `--replay-wal`), returning
/// its digest, stats summary and the intent file it wrote.
fn run(src_flag: &str, src: &Path, out: &Path, wal: Option<&Path>) -> Run {
    let _ = std::fs::remove_file(out);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_arb-trader"));
    cmd.arg(src_flag)
        .arg(src)
        .arg("--registry")
        .arg(data("synth-registry.yaml"))
        .arg("--out")
        .arg(out)
        // never let a stray data/KILL in the workspace perturb a gate
        .arg("--kill-file")
        .arg(out.with_extension("no-such-kill-file"));
    if let Some(w) = wal {
        let _ = std::fs::remove_file(w); // WAL opens append-only by design
        cmd.arg("--wal").arg(w);
    }
    let o = cmd.output().expect("run arb-trader");
    assert!(o.status.success(), "arb-trader failed: {}", String::from_utf8_lossy(&o.stderr));
    let stdout = String::from_utf8(o.stdout).expect("utf8");
    let last = stdout.lines().last().expect("summary line");
    let summary: serde_json::Value = serde_json::from_str(last).expect("summary json");
    Run {
        digest: summary["sha256"].as_str().expect("bench digest").to_owned(),
        summary,
        intents: std::fs::read_to_string(out).expect("read intents"),
        stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
    }
}

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("arb-trader-test-{name}"))
}

#[test]
fn synthetic_tape_is_a_deterministic_fold() {
    let tape = data("synth-tape.jsonl");
    let a = run("--bench-tape", &tape, &tmp("det-a.jsonl"), None);
    let b = run("--bench-tape", &tape, &tmp("det-b.jsonl"), None);
    assert_eq!(a.digest, b.digest, "same tape, same digest");
    assert_eq!(a.intents, b.intents, "same tape, byte-identical intent stream");

    // the fill path actually ran (guards against a silently-skipped kind).
    // FOUR, not five: the tape's 5th fill frame is for `ghost-oid`, which maps to
    // no order of ours. `fills` counts frames attributable to a maker order of
    // ours; counting a foreign fill there over-reported the gauge and would have
    // double-counted any frame replayed out of the unattributed hold.
    assert_eq!(a.summary["fills"], 4);
    assert_eq!(a.summary["fills_unattributed"], 1, "the ghost is counted HERE");
    assert_eq!(a.summary["order_acks"], 2);
    assert_eq!(a.summary["hedge_obligations"], 3);
    assert_eq!(a.summary["dropped_unconsumed"], 0, "every obligation was consumed");

    let hedges: Vec<&str> =
        a.intents.lines().filter(|l| l.contains("\"hedge_needed\"")).collect();
    assert_eq!(
        hedges,
        vec![
            // cum 2 on m1 mints 2; the duplicate cum-2 report mints nothing
            r#"{"anchor_price":"0.40","hedge_needed":"SYNTH-P-YES","order_id":"m1","qty":2,"ts":1700000003.0}"#,
            // cum 4 on m1 AFTER m3 replaced it: a fill racing the cancel still
            // needs its hedge — delta 2, not 4
            r#"{"anchor_price":"0.40","hedge_needed":"SYNTH-P-YES","order_id":"m1","qty":2,"ts":1700000021.0}"#,
            // m3's anchor is its PLACE-time hedge top (0.40) even though the
            // hedge leg moved to 0.36 before this fill arrived, and the
            // ghost-oid fill after it mints nothing
            r#"{"anchor_price":"0.40","hedge_needed":"SYNTH-P-YES","order_id":"m3","qty":5,"ts":1700000031.0}"#,
        ],
        "hedge obligation lines are canonical and sorted-key"
    );
}

#[test]
fn wal_replay_reproduces_the_run_that_wrote_it() {
    let tape = data("synth-tape.jsonl");
    let wal = tmp("wal.jsonl");
    let orig = run("--bench-tape", &tape, &tmp("wal-orig.jsonl"), Some(&wal));

    // every merged event is in the WAL, stamped in engine order
    let wal_txt = std::fs::read_to_string(&wal).expect("read wal");
    let seqs: Vec<u64> = wal_txt
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("wal json")["seq"]
            .as_u64()
            .expect("seq"))
        .collect();
    assert_eq!(seqs, (1..=orig.summary["events"].as_u64().unwrap()).collect::<Vec<_>>());

    let replayed = run("--replay-wal", &wal, &tmp("wal-replay.jsonl"), None);
    assert_eq!(orig.digest, replayed.digest, "WAL replay reproduces the digest");
    assert_eq!(orig.intents, replayed.intents, "WAL replay is byte-identical");
    assert_eq!(orig.summary["fills"], replayed.summary["fills"]);
    assert_eq!(orig.summary["hedge_obligations"], replayed.summary["hedge_obligations"]);
}

#[test]
fn logging_a_wal_does_not_change_decisions() {
    let tape = data("synth-tape.jsonl");
    let without = run("--bench-tape", &tape, &tmp("nowal.jsonl"), None);
    let with = run("--bench-tape", &tape, &tmp("yeswal.jsonl"), Some(&tmp("neutral.wal")));
    assert_eq!(without.digest, with.digest, "--wal is decision-neutral");
    assert_eq!(without.intents, with.intents);
}

// ------------------------------------------------------------ hedge placing ---

fn hedge_lines(intents: &str) -> Vec<serde_json::Value> {
    intents
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("tag").and_then(|t| t.as_str()) == Some("hedge"))
        .collect()
}

/// A maker fill must produce a real hedge ORDER, not just an obligation line:
/// on the OTHER venue, on the opposite side, marketable, for the fill's size.
#[test]
fn a_maker_fill_places_a_taker_hedge_on_the_other_leg() {
    let tape = data("synth-tape.jsonl");
    let r = run("--bench-tape", &tape, &tmp("hedge-place.jsonl"), None);
    let hedges = hedge_lines(&r.intents);
    assert_eq!(hedges.len(), 3, "one hedge per obligation: {:?}", hedges);

    let h = &hedges[0];
    assert_eq!(h["venue"], "polymarket_us", "the OTHER leg — the maker filled on kalshi");
    assert_eq!(h["place"], "SYNTH-P-YES");
    assert_eq!(h["side"], "ask", "a filled maker BID leaves us long, so the hedge SELLS");
    assert_eq!(h["taker"], true, "a hedge crosses; it never rests");
    assert_eq!(h["count"], 2, "the fill's size, not the resting size");
    assert_eq!(h["order_id"], "h1", "hedge ids are their own series — never collide with makers");
}

/// The book at fill time is the book the filling burst just gapped, so a
/// missing level falls back to the anchor captured at PLACE time.
#[test]
fn a_hedge_prices_at_the_touch_and_falls_back_to_the_anchor() {
    let tape = data("synth-tape.jsonl");
    let r = run("--bench-tape", &tape, &tmp("hedge-px.jsonl"), None);
    let hedges = hedge_lines(&r.intents);
    // PM's bid is 0.40 when the first two fire, and 0.36 by the third.
    assert_eq!(hedges[0]["price"], "0.40");
    assert_eq!(hedges[2]["price"], "0.36", "the CURRENT touch once the book moved");
}

/// THE loop guard. A hedge is not in the FillLedger, so its own fill mints
/// nothing — otherwise every hedge would hedge itself, forever.
#[test]
fn a_hedge_fill_never_mints_another_hedge() {
    let tape = data("synth-hedge-loop.jsonl");
    let r = run("--bench-tape", &tape, &tmp("hedge-loop.jsonl"), None);
    let hedges = hedge_lines(&r.intents);
    assert_eq!(
        hedges.len(),
        1,
        "one maker fill => exactly one hedge, and the hedge's own fills add none: {:?}",
        hedges
    );
    assert_eq!(
        r.summary["hedge_obligations"], 1,
        "the hedge's fills must not register as obligations"
    );
}

/// Every quantity the engine says it BOOKED, in order. Bench never writes the
/// real ledger, so this line is the only place the booked size is visible.
fn booked_quantities(stderr: &str) -> Vec<i64> {
    stderr
        .lines()
        .filter(|l| l.starts_with("[ledger] booked "))
        .filter_map(|l| l.split(" x").nth(1)?.split_whitespace().next()?.parse().ok())
        .collect()
}

/// THE C3 regression, end to end. A 10-lot… here a 5-lot hedge fills 2 then 5
/// (cumulative, two frames), which is the NORMAL shape: Kalshi sends one frame
/// per trade and PM-US sends PARTIAL_FILL then FILL.
///
/// Before 2026-07-28 the engine did `hedge_orders.remove(oid)` on the FIRST
/// frame, so frame two matched no hedge, fell through to the maker path, found
/// nothing in the FillLedger (hedges are deliberately never registered there)
/// and was dropped with no alarm. It booked 2 of 5, understated exposure
/// forever, and the remaining 3 lost both its retry and its naked alarm.
#[test]
fn a_hedge_filling_in_two_frames_books_its_whole_size() {
    let r = run(
        "--bench-tape",
        &data("synth-hedge-partial.jsonl"),
        &tmp("hedge-partial.jsonl"),
        None,
    );
    let booked = booked_quantities(&r.stderr);
    assert_eq!(booked, vec![2, 3], "each frame books its own delta: {:?}", booked);
    assert_eq!(booked.iter().sum::<i64>(), 5, "and they add up to the hedge that filled");
    assert_eq!(
        r.summary["hedges_pending"], 0,
        "the obligation is retired only once it is really covered"
    );
    assert_eq!(r.summary["hedges_overfilled"], 0);
    assert_eq!(
        r.summary["fills_unattributed"], 0,
        "no frame of a hedge we placed may read as unexplained money — including the \
         duplicate third frame, which is idempotent"
    );
    assert_eq!(r.summary["hedge_obligations"], 1, "one maker fill, one obligation");
}

/// A fill that beats its own `order_ack` must not be lost. The ack is emitted
/// once the place's HTTP response returns while the fill arrives on the venue's
/// private socket, and Kalshi's fill channel carries only Kalshi's own order id —
/// so for a moment the fill maps to nothing. The fixture uses the observed 48 ms
/// margin.
///
/// Dropping it meant no hedge was minted, `record_open` never fired (so the
/// concentration cap never bound), and on a hedge's own fill the 5-second retry
/// bought the hedge a second time.
#[test]
fn a_fill_that_arrives_before_its_ack_is_still_hedged() {
    let r = run(
        "--bench-tape",
        &data("synth-fill-before-ack.jsonl"),
        &tmp("fill-before-ack.jsonl"),
        None,
    );
    assert_eq!(
        r.summary["hedge_obligations"], 1,
        "the held fill is replayed the moment its ack names it"
    );
    let hedges = hedge_lines(&r.intents);
    assert_eq!(hedges.len(), 1, "exactly one hedge, never zero and never two: {:?}", hedges);
    assert_eq!(hedges[0]["count"], 2, "for the size that actually filled");
    assert_eq!(hedges[0]["place"], "SYNTH-P-YES", "on the other leg");
    assert_eq!(r.summary["fills_unclaimed"], 0, "nothing left holding");
    assert_eq!(r.summary["fills_unattributed"], 0, "and nothing unexplained");
    assert!(
        r.stderr.contains("replaying the held fill"),
        "the replay must be visible in the log: {}",
        r.stderr
    );
}

/// A fill the engine cannot attribute must be LOUD. The synthetic tape ends with
/// a fill for `ghost-oid`; it used to be counted in `fills` and thrown away in
/// silence, which is exactly the shape of a real naked position with zero
/// detection (a place returns a parse error, the order rests anyway, no ack is
/// ever emitted, and the fill arrives under the venue's id).
#[test]
fn an_unattributable_fill_alarms_instead_of_vanishing() {
    let r = run("--bench-tape", &data("synth-tape.jsonl"), &tmp("ghost-fill.jsonl"), None);
    assert_eq!(
        r.summary["fills_unattributed"], 1,
        "the ghost fill is counted as unexplained, not just counted"
    );
    assert!(r.stderr.contains("UNATTRIBUTED 1x on kalshi SYNTH-K-YES"), "{}", r.stderr);
    assert!(
        r.stderr.contains("RECONCILE BY HAND"),
        "an unexplained fill must name what a human has to do: {}",
        r.stderr
    );
    assert_eq!(r.summary["dropped_unconsumed"], 0, "and it is not a dropped obligation");
}

/// A dry run must NEVER write to the accounting ledger. It would invent open
/// baskets that the next startup seeds exposure from, quietly shrinking the
/// caps against positions that do not exist.
#[test]
fn a_dry_run_never_books_a_basket() {
    let ledger = tmp("must-not-exist-ledger.jsonl");
    let _ = std::fs::remove_file(&ledger);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_arb-trader"));
    let out = tmp("dryrun-ledger.jsonl");
    let _ = std::fs::remove_file(&out);
    let o = cmd
        .arg("--bench-tape")
        .arg(data("synth-tape.jsonl"))
        .arg("--registry")
        .arg(data("synth-registry.yaml"))
        .arg("--out")
        .arg(&out)
        .arg("--ledger")
        .arg(&ledger)
        .arg("--kill-file")
        .arg(out.with_extension("no-such-kill-file"))
        .output()
        .expect("run arb-trader");
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));

    // the tape contains maker fills, so an armed engine WOULD have booked
    assert!(
        !ledger.exists(),
        "an unarmed engine wrote to the trade ledger at {}",
        ledger.display()
    );
}
