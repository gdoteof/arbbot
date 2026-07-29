//! The `SHADOW GATE: NO VERDICT` sentinel, pinned by running the real binary.
//!
//! It is the whole fix for "a killed run leaves a report with no verdict line",
//! which is byte-for-byte the operator signal the three 153-byte `can't open
//! file` reports gave and which the runbook's soak grep cannot distinguish from
//! "the gate was never installed". It had no test: deleting the `println!`
//! turned nothing red.
//!
//! A unit test cannot pin it, because the thing being asserted is what reaches
//! STDOUT of a process that then exits abnormally. So this is an integration
//! test that spawns the binary and reads its output — which also pins the
//! ORDERING that a unit test could not see at all: the sentinel has to be
//! printed before `parse_args()`, because `parse_args` exits 2 on an unknown
//! flag and panics on a flag missing its value. With `tee -a` in the unit
//! (deliberate: a hand-run must not erase the timer's report), either exit
//! leaves the PREVIOUS run's `PASS` as the last `SHADOW GATE:` line in the
//! day's file. A typo'd hand-run would make a day look green.
//!
//! Neither case here touches a tape, a socket or a CPU: both die inside
//! argument parsing.

use std::process::Command;

const EXE: &str = env!("CARGO_BIN_EXE_arb-shadow-gate");

/// The last `SHADOW GATE:` line of a report is that report's verdict — the
/// runbook's soak grep is `grep 'SHADOW GATE' "$f" | tail -1`. This is that
/// grep.
fn last_verdict_line(stdout: &str) -> Option<&str> {
    stdout.lines().rev().find(|l| l.contains("SHADOW GATE:"))
}

#[test]
fn an_unknown_flag_still_leaves_a_no_verdict_line_behind() {
    let out = Command::new(EXE).arg("--not-a-real-flag").output().expect("spawn the gate");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(2), "an unknown flag must exit 2: {stdout}");
    assert_eq!(
        last_verdict_line(&stdout),
        Some("SHADOW GATE: NO VERDICT — this run has not finished"),
        "the last SHADOW GATE line of a run that died in parse_args must be NO VERDICT, \
         or `tee -a` leaves the previous run's PASS standing as this day's verdict. \
         stdout was: {stdout:?}"
    );
}

#[test]
fn a_flag_missing_its_value_still_leaves_a_no_verdict_line_behind() {
    // `--day` with nothing after it panics in `parse_args`. Abnormal exit, and
    // the sentinel has to have reached stdout before it.
    let out = Command::new(EXE).arg("--day").output().expect("spawn the gate");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_ne!(out.status.code(), Some(0), "a flag with no value must not exit 0");
    assert_eq!(
        last_verdict_line(&stdout),
        Some("SHADOW GATE: NO VERDICT — this run has not finished"),
        "a panic in parse_args must still leave NO VERDICT behind. stdout was: {stdout:?}"
    );
}
