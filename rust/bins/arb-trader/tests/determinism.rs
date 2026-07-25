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

    // the fill path actually ran (guards against a silently-skipped kind)
    assert_eq!(a.summary["fills"], 5);
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
