//! arb-gauges — the armed engine's safety gauges, judged every watchdog poll.
//!
//!   arb-gauges STATE PENDING_DELIVERED PENDING_HELD INVOCATION_ID < stats-line
//!
//! `scripts/freshness_check.sh` pipes in the newest stats line arbbot-trader-m3
//! printed (one JSON object, `engine/mod.rs::summary`) and passes systemd's
//! InvocationID for the unit, empty when systemd has none. The rules, and the
//! reasoning behind each threshold, are in [`gauges`].
//!
//! Output is one line per verdict, held verdicts first:
//!   PAGE|<message>    money, or something that hides money
//!   NOTE|<message>    worth one low-priority line
//!
//! STATE is read and never written. The next state is written TWICE, as two
//! candidates, because only the caller knows whether its alert went out:
//!   PENDING_DELIVERED  baseline advanced, nothing held
//!   PENDING_HELD       baseline advanced, this poll's verdicts kept to repeat
//! The caller moves exactly one over STATE. Why the text is held rather than
//! the baseline is in the [`gauges`] module doc.
//!
//! Exit 0 whenever the stats line was judged, with verdicts or without. Exit 2
//! on a usage error. Exit 1 when it could not be judged — the line is not JSON,
//! a value a rule reads has the wrong type, or a candidate could not be written
//! — and the caller pages "gauge check FAILED to run" and adopts neither
//! candidate. An unreadable STATE is NOT one of those: it is a cold start.

mod gauges;
mod value;

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::exit;

use value::{Fault, Obj, Value};

const USAGE: &str =
    "usage: arb-gauges STATE PENDING_DELIVERED PENDING_HELD INVOCATION_ID < stats-line";

fn main() {
    exit(run());
}

fn run() -> i32 {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let [state, pend_ok, pend_held, invocation] = args.as_slice() else {
        eprintln!("{USAGE}");
        return 2;
    };
    // systemd's InvocationID for the unit: a fresh UUID per start, and the only
    // exact process identity available here. REQUIRED as an argument (empty
    // string when systemd cannot supply one) rather than optional, so a caller
    // that forgets it fails loudly instead of silently falling back.
    let Some(invocation) = invocation.to_str() else {
        eprintln!("arb-gauges: INVOCATION_ID is not UTF-8\n{USAGE}");
        return 2;
    };
    let invocation = strip(invocation);

    let cur = match read_stats() {
        Ok(cur) => cur,
        Err(e) => {
            eprintln!("arb-gauges: {e}");
            return 1;
        }
    };
    let prev = load_state(Path::new(state));
    let poll = match gauges::evaluate(&cur, &prev, invocation) {
        Ok(poll) => poll,
        Err(f) => return fault(&f),
    };
    let mut stdout = io::stdout().lock();
    for line in poll.lines() {
        if let Err(e) = writeln!(stdout, "{line}") {
            eprintln!("arb-gauges: stdout: {e}");
            return 1;
        }
    }
    if let Err(e) = stdout.flush() {
        eprintln!("arb-gauges: stdout: {e}");
        return 1;
    }
    let (delivered, held) = match poll.next_states(&cur, invocation) {
        Ok(states) => states,
        Err(f) => return fault(&f),
    };
    for (path, next) in [(pend_ok, &delivered), (pend_held, &held)] {
        let path = Path::new(path);
        if let Err(e) = write_state(path, next) {
            eprintln!("arb-gauges: cannot write {}: {e}", path.display());
            return 1;
        }
    }
    0
}

fn fault(f: &Fault) -> i32 {
    eprintln!("arb-gauges: cannot judge this stats line against this state: {}", f.0);
    1
}

/// Python's `str.strip()`: Unicode whitespace, plus the four ASCII
/// information separators (U+001C..U+001F) Python also counts as space.
fn strip(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}

fn read_stats() -> Result<Value, String> {
    let mut raw = Vec::new();
    io::stdin().read_to_end(&mut raw).map_err(|e| format!("stdin: {e}"))?;
    let text = String::from_utf8(raw).map_err(|e| format!("stats line is not UTF-8: {e}"))?;
    value::parse(&text).map_err(|e| format!("stats line is not JSON: {e}"))
}

/// The previous baseline, or an empty one — which means COLD START, never a
/// failure.
///
/// Falling back to a cold start on an unreadable state file is the whole
/// recovery story and it must never be an exit: a non-zero exit makes
/// freshness_check.sh page "gauge check FAILED to run" every 30 minutes AND
/// read no gauge at all, until a human deletes the file by hand. Loud and blind
/// at once, with manual-only recovery, is strictly worse than the gap this
/// closes. What falls back here is a file that cannot be READ as an object; one
/// that parses but holds a wrong-typed field faults in the rule that reads it.
/// No writer of this format produces such a file.
fn load_state(path: &Path) -> Obj {
    let unreadable = |why: &dyn fmt::Display| {
        eprintln!(
            "arb-gauges: {} unreadable ({why}) — treating as a COLD START. This poll reports \
             nothing and reseeds; the next one is normal.",
            path.display()
        );
        Obj::default()
    };
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Obj::default(),
        Err(e) => return unreadable(&e),
    };
    let text = match String::from_utf8(raw) {
        Ok(text) => text,
        Err(e) => return unreadable(&e),
    };
    match value::parse(&text) {
        Ok(Value::Obj(prev)) => prev,
        Ok(_) => {
            eprintln!("arb-gauges: {} is not an object — COLD START.", path.display());
            Obj::default()
        }
        Err(e) => unreadable(&e),
    }
}

/// Same-directory temp file + atomic rename.
///
/// Writing in place truncates first, so any death in that window leaves a
/// partial file that the next run would adopt as a baseline.
fn write_state(path: &Path, state: &Value) -> io::Result<()> {
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    let (tmp, mut file) = create_temp(dir)?;
    let written = file
        .write_all(state.to_string().as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| fs::rename(&tmp, path));
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    written
}

/// A new file readable by this user only, under a name no other writer holds.
fn create_temp(dir: &Path) -> io::Result<(PathBuf, File)> {
    let pid = std::process::id();
    for attempt in 0..100 {
        let tmp = dir.join(format!(".gauge-{pid}-{attempt}.tmp"));
        match OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp) {
            Ok(file) => return Ok((tmp, file)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("100 temp names in {} are all taken", dir.display()),
    ))
}

#[cfg(test)]
mod tests {
    use super::strip;

    #[test]
    fn strips_what_python_str_strip_strips() {
        assert_eq!(strip(" \t\u{1c}abc\u{1f}\u{a0}\n"), "abc");
        assert_eq!(strip("a b"), "a b");
    }
}
