//! The architecture view — a diagram DERIVED from the running system.
//!
//! An architecture picture drawn by hand is wrong the week after it is drawn,
//! and it never says so. This one is assembled per request out of three
//! sources, only the first of which is written by hand:
//!
//!   * the TOPOLOGY below — the components, what each is for, and the edges
//!     between them. This is the one place to edit when something is added or
//!     retired, and it is deliberately the only place.
//!   * the RUNNING PROCESSES, read from `/proc`: which unit each belongs to
//!     (its cgroup), how long it has been up (a pid directory's mtime IS the
//!     process start time), and the command line it is ACTUALLY running. That
//!     last one matters here more than anywhere: the armed engine's
//!     `--enable-orders --yes-trade-live` lives in a drop-in that is
//!     deliberately not in the repo, so arming can only be read off the
//!     process, never off a file this repo tracks.
//!   * the ARTIFACTS on disk, whose mtimes are the only evidence an edge is
//!     carrying anything.
//!
//! Drift is therefore loud rather than silent, in all four directions:
//!
//!   * a declared unit that is not running is RED;
//!   * a unit that IS running and is not declared here is listed as
//!     undeclared — the diagram admits it does not know about it;
//!   * a node declared OFF whose file keeps growing is reported, because a
//!     component nobody believes is on is the one nobody is watching;
//!   * a timer that is enabled and active with no next elapse is DOWN, not
//!     green (the monotonic-timer trap: re-enabling an `OnBootSec` timer
//!     leaves it looking healthy and never firing).
//!
//! Read-only throughout: it stats files, reads `/proc`, and asks `systemctl`
//! two questions. It cannot start, stop or change anything.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::UNIX_EPOCH;

use arb_core::clock::now_secs;
use serde_json::{json, Value};

/// What the topology says a node is SUPPOSED to be doing. The value of
/// declaring it is `Off`: without it, a retired component and a dead one look
/// identical, and so do a retired component and one that quietly came back.
#[derive(PartialEq, Clone, Copy)]
enum Expect {
    Live,
    Off,
}

/// Where a node's liveness comes from.
enum Probe {
    /// Its unit's process. What is not a file and not a unit of its own — the
    /// socket, the in-process order sink — borrows the liveness of whatever
    /// owns it, rather than inventing one.
    Unit,
    /// One file's mtime.
    File(&'static str),
    /// The newest `<prefix>…` file in a directory: the shape every per-day
    /// artifact in this repo takes.
    Newest(&'static str, &'static str),
    /// A systemd timer. Live means SCHEDULED — see the monotonic trap above.
    Timer,
}

struct Decl {
    id: &'static str,
    label: &'static str,
    /// Second line on the card: the unit, the path, or the protocol.
    sub: &'static str,
    lane: u8,
    row: u8,
    /// `venue` | `process` | `timer` | `artifact` | `part`
    kind: &'static str,
    unit: Option<&'static str>,
    probe: Probe,
    /// Older than this and the node is stale. `None` reports the age and never
    /// judges it — for artifacts that are legitimately quiet for days.
    max_age_s: Option<u64>,
    expect: Expect,
    /// One sentence: what this is, or the thing about it worth knowing.
    what: &'static str,
}

const LANES: [&str; 6] = ["venues", "record", "tape", "decide", "execute", "observe"];

/// The topology. EDIT HERE when a component arrives or leaves — everything
/// else on the page is measured.
const NODES: &[Decl] = &[
    // ---- venues -----------------------------------------------------------
    Decl {
        id: "kalshi", label: "Kalshi", sub: "WS books · REST orders",
        lane: 0, row: 1, kind: "venue", unit: None,
        probe: Probe::Newest("data/raw", "kalshi-"), max_age_s: Some(300), expect: Expect::Live,
        what: "CFTC-regulated DCM, and the one venue this stack has always been able to trade.",
    },
    Decl {
        id: "pmus", label: "Polymarket US", sub: "WS books · REST orders",
        lane: 0, row: 2, kind: "venue", unit: None,
        probe: Probe::Newest("data/raw", "polymarket_us-"), max_age_s: Some(300),
        expect: Expect::Live,
        what: "The regulated US book. Its market universe comes from polymarket_us_tags, \
               not from the registry, so most of what it records backs no pair.",
    },
    Decl {
        id: "pmintl", label: "Polymarket INTL", sub: "data only · geoblocked",
        lane: 0, row: 4, kind: "venue", unit: None,
        // A bound even though nothing here is required to be fresh: without
        // one, `off` and `still recording` are the same reading, and the
        // drift check below has nothing to fire on.
        probe: Probe::Newest("data/raw", "polymarket-"), max_age_s: Some(600),
        expect: Expect::Off,
        what: "No order path — placement is geoblocked from a US IP. Recording turned off \
               2026-07-31 (record_polymarket_intl: false) after its sweep produced 58 of 64 \
               ntfy alerts in six hours.",
    },
    // ---- record -----------------------------------------------------------
    Decl {
        id: "recorder", label: "arb-recorder", sub: "arbbot-recorder.service",
        lane: 1, row: 2, kind: "process", unit: Some("arbbot-recorder.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "The single writer of market data: one connection per venue, one append-only \
               tape per venue-day, and one socket that fans the same frames out to every \
               subscriber. Seeds each book over REST at connect, so the book set is not \
               merely 'whatever has ticked since we started'.",
    },
    // ---- tape -------------------------------------------------------------
    Decl {
        id: "sock", label: "data/arbbot.sock", sub: "unix socket · line JSON",
        lane: 2, row: 1, kind: "artifact", unit: Some("arbbot-recorder.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "The fan-out. Every consumer sees the same frames in the same order; a slow \
               subscriber is evicted rather than allowed to back the recorder up.",
    },
    Decl {
        id: "tape", label: "data/raw/*.jsonl", sub: "append-only, one file per venue-day",
        lane: 2, row: 3, kind: "artifact", unit: None,
        probe: Probe::Newest("data/raw", ""), max_age_s: Some(300), expect: Expect::Live,
        what: "The authoritative record. Crash-safe and greppable while a day is open; \
               archived to Parquet once it closes.",
    },
    Decl {
        id: "health", label: "data/health.jsonl", sub: "per-feed staleness",
        lane: 2, row: 4, kind: "artifact", unit: None,
        probe: Probe::File("data/health.jsonl"), max_age_s: Some(120), expect: Expect::Live,
        what: "What the engine reads before it quotes. `stale` is the live flag; a feed that \
               never connected is ABSENT from it, not false.",
    },
    // ---- decide -----------------------------------------------------------
    Decl {
        id: "registry", label: "registry.yaml + tradable.yaml", sub: "the trading gate",
        lane: 3, row: 0, kind: "artifact", unit: None,
        probe: Probe::File("config/registry.yaml"), max_age_s: None, expect: Expect::Live,
        what: "The vetted pair set. A relationship trades only with vetted_by: human or an \
               entry in tradable.yaml, and that stamp is never applied programmatically. \
               Gitignored: it is the trading pair set.",
    },
    Decl {
        id: "trader_m3", label: "arb-trader", sub: "arbbot-trader-m3.service",
        lane: 3, row: 1, kind: "process", unit: Some("arbbot-trader-m3.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "The production engine. Books, quoters, risk caps, hedge obligations and the \
               order sink all live in ONE single-writer task with no locks; venue I/O happens \
               in per-venue executors so a slow venue can never stall the reader.",
    },
    Decl {
        id: "trader_rs", label: "arb-trader (shadow)", sub: "arbbot-trader-rs.service",
        lane: 3, row: 2, kind: "process", unit: Some("arbbot-trader-rs.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "The same binary with no order path and no credentials loaded. It prices the \
               whole registry, so intents can be read without arming anything.",
    },
    Decl {
        id: "scanner", label: "scan.daemon (Python)", sub: "arbbot-scanner.service",
        lane: 3, row: 3, kind: "process", unit: Some("arbbot-scanner.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "The last Python daemon on the live path. It subscribes to the same socket and \
               writes the research tape the opportunity views read; it places nothing.",
    },
    Decl {
        id: "marks", label: "data/exec/marks.json", sub: "position marks",
        lane: 3, row: 5, kind: "artifact", unit: None,
        probe: Probe::File("data/exec/marks.json"), max_age_s: Some(900), expect: Expect::Live,
        what: "The armed engine WRITES this file and reads its take-take APR bar back out of \
               it — arbbot-marks.timer was retired in the same change. Past 900s the bar \
               returns Untrusted and take-take refuses rather than firing on a stale number.",
    },
    // ---- execute ----------------------------------------------------------
    Decl {
        id: "sink", label: "order sink", sub: "in arb-trader · arb-venue gateways",
        lane: 4, row: 0, kind: "part", unit: Some("arbbot-trader-m3.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "Where an effect stops being a record and becomes a wire. One executor per \
               venue, each with its own token bucket; dry-run is the default and a sink is \
               only ever constructed by --enable-orders once its preconditions pass.",
    },
    Decl {
        id: "wal", label: "m3-wal.jsonl", sub: "engine-sequenced WAL",
        lane: 4, row: 1, kind: "artifact", unit: None,
        probe: Probe::File("data/trader-rs/m3-wal.jsonl"), max_age_s: Some(300),
        expect: Expect::Live,
        what: "Every event the engine saw, in the order it saw it. Replaying it must \
               reproduce the original run's digest byte for byte — that is what makes an \
               incident reconstructable.",
    },
    Decl {
        id: "m3_intents", label: "m3-intents.jsonl", sub: "armed engine decisions",
        lane: 4, row: 2, kind: "artifact", unit: None,
        probe: Probe::File("data/trader-rs/m3-intents.jsonl"), max_age_s: Some(3600),
        expect: Expect::Live,
        what: "What the armed engine decided. The Live tape reads this; the Intents view is \
               pointed at the shadow file below, so the two views are not about the same \
               engine.",
    },
    Decl {
        id: "rs_intents", label: "intents.jsonl", sub: "shadow engine decisions",
        lane: 4, row: 3, kind: "artifact", unit: None,
        probe: Probe::File("data/trader-rs/intents.jsonl"), max_age_s: Some(3600),
        expect: Expect::Live,
        what: "The dry-run engine's intents, and what --intents points arb-dash at.",
    },
    Decl {
        id: "ledger", label: "data/exec/trades.jsonl", sub: "append-only record of money",
        lane: 4, row: 4, kind: "artifact", unit: None,
        probe: Probe::File("data/exec/trades.jsonl"), max_age_s: None, expect: Expect::Live,
        what: "Closing a basket never rewrites a line — it appends a compensating record — so \
               what is open is a fold, not a lookup. The engine reloads it at startup or risk \
               caps reset to an empty book on every restart. Quiet for days is normal.",
    },
    Decl {
        id: "scan", label: "data/scan/*.jsonl", sub: "research tape",
        lane: 4, row: 5, kind: "artifact", unit: None,
        probe: Probe::Newest("data/scan", "lifetimes-"), max_age_s: Some(3600),
        expect: Expect::Live,
        what: "Quote lifetimes, maker fills and the sports equivalence map — the inputs to \
               every backtest, and to the opportunity views on this dashboard.",
    },
    // ---- observe ----------------------------------------------------------
    Decl {
        id: "watchdog", label: "freshness watchdog", sub: "arbbot-watchdog.timer · 5 min",
        lane: 5, row: 0, kind: "timer", unit: Some("arbbot-watchdog.timer"),
        probe: Probe::Timer, max_age_s: None, expect: Expect::Live,
        what: "Alerts to ntfy when output goes stale while the recorder is alive. It watched \
               only the Python stack until 2026-07-29; the armed engine could have stopped \
               dead without a sound.",
    },
    Decl {
        id: "dash_rs", label: "arb-dash", sub: "arbbot-dash-rs.service · you are here",
        lane: 5, row: 1, kind: "process", unit: Some("arbbot-dash-rs.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "Binds 127.0.0.1 only, holds no credentials, and cannot place or cancel an \
               order. Read-only with one exception: POST /api/rollup rebuilds the local ToB \
               series.",
    },
    Decl {
        id: "dash_py", label: "Python dash", sub: "arbbot-dash.service · :4748",
        lane: 5, row: 2, kind: "process", unit: Some("arbbot-dash.service"),
        probe: Probe::Unit, max_age_s: None, expect: Expect::Live,
        what: "The older instrument panel, still up on its own port so numbers can be \
               compared side by side.",
    },
    Decl {
        id: "report", label: "nightly ETL + report", sub: "arbbot-report.timer · 07:30",
        lane: 5, row: 3, kind: "timer", unit: Some("arbbot-report.timer"),
        probe: Probe::Timer, max_age_s: None, expect: Expect::Live,
        what: "Archives closed days to Parquet and writes the daily report. Every step is \
               piped to /dev/null, which is how polymarket_us once failed to archive for a \
               week in silence — the Recording view exists to catch that.",
    },
    Decl {
        id: "parquet", label: "data/parquet", sub: "archived closed days",
        lane: 5, row: 4, kind: "artifact", unit: None,
        probe: Probe::Newest("data/parquet", ""), max_age_s: Some(172_800), expect: Expect::Live,
        what: "The read format: ~25x smaller than the JSONL it replaces, and the reason a \
               one-relationship query is milliseconds instead of minutes.",
    },
    Decl {
        id: "settle", label: "settlement sweep", sub: "arbbot-settle.timer · hourly",
        lane: 5, row: 5, kind: "timer", unit: Some("arbbot-settle.timer"),
        probe: Probe::Timer, max_age_s: None, expect: Expect::Live,
        what: "Turns settled baskets into realized P&L by appending to the ledger.",
    },
    Decl {
        id: "sportsmap", label: "sports equivalence map", sub: "arbbot-sports-map.timer · 6h",
        lane: 5, row: 6, kind: "timer", unit: Some("arbbot-sports-map.timer"),
        probe: Probe::Timer, max_age_s: None, expect: Expect::Live,
        what: "Rematches the two venues' sports calendars into candidate pairs. Candidates \
               only: nothing here reaches the engine without a human vetting stamp.",
    },
];

struct Edge {
    from: &'static str,
    to: &'static str,
    label: &'static str,
    /// `feed` (market data) | `write` | `read` | `order` (the money path)
    kind: &'static str,
}

const EDGES: &[Edge] = &[
    Edge { from: "kalshi", to: "recorder", label: "WS books", kind: "feed" },
    Edge { from: "pmus", to: "recorder", label: "WS books", kind: "feed" },
    Edge { from: "pmintl", to: "recorder", label: "off", kind: "feed" },
    Edge { from: "recorder", to: "sock", label: "fan-out", kind: "write" },
    Edge { from: "recorder", to: "tape", label: "append", kind: "write" },
    Edge { from: "recorder", to: "health", label: "liveness", kind: "write" },
    Edge { from: "sock", to: "trader_m3", label: "frames", kind: "feed" },
    Edge { from: "sock", to: "trader_rs", label: "frames", kind: "feed" },
    Edge { from: "sock", to: "scanner", label: "frames", kind: "feed" },
    Edge { from: "registry", to: "trader_m3", label: "vetted pairs", kind: "read" },
    Edge { from: "registry", to: "trader_rs", label: "vetted pairs", kind: "read" },
    Edge { from: "health", to: "trader_m3", label: "quote gate", kind: "read" },
    Edge { from: "trader_m3", to: "marks", label: "writes", kind: "write" },
    Edge { from: "marks", to: "trader_m3", label: "take-take bar", kind: "read" },
    Edge { from: "trader_m3", to: "sink", label: "place / cancel", kind: "order" },
    Edge { from: "sink", to: "kalshi", label: "orders", kind: "order" },
    Edge { from: "sink", to: "pmus", label: "orders", kind: "order" },
    Edge { from: "trader_m3", to: "wal", label: "every event", kind: "write" },
    Edge { from: "trader_m3", to: "m3_intents", label: "decisions", kind: "write" },
    Edge { from: "trader_m3", to: "ledger", label: "booked baskets", kind: "write" },
    Edge { from: "ledger", to: "trader_m3", label: "open book at start", kind: "read" },
    Edge { from: "trader_rs", to: "rs_intents", label: "decisions", kind: "write" },
    Edge { from: "scanner", to: "scan", label: "lifetimes / maker", kind: "write" },
    Edge { from: "m3_intents", to: "dash_rs", label: "live tape", kind: "read" },
    Edge { from: "rs_intents", to: "dash_rs", label: "intents view", kind: "read" },
    Edge { from: "ledger", to: "dash_rs", label: "trades view", kind: "read" },
    Edge { from: "scan", to: "dash_rs", label: "opportunities", kind: "read" },
    Edge { from: "parquet", to: "dash_rs", label: "history", kind: "read" },
    Edge { from: "tape", to: "report", label: "closed days", kind: "read" },
    Edge { from: "report", to: "parquet", label: "archive", kind: "write" },
    Edge { from: "health", to: "watchdog", label: "→ ntfy", kind: "read" },
    Edge { from: "settle", to: "ledger", label: "realized P&L", kind: "write" },
    Edge { from: "sportsmap", to: "scan", label: "candidate pairs", kind: "write" },
];

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

/// One running process, as `/proc` reports it.
///
/// `pub(crate)` for the Now view, which needs a unit's start time to date the
/// summary line it reads out of the journal.
#[derive(Clone)]
pub struct Proc {
    pub pid: u32,
    pub up_s: u64,
    pub cmd: String,
}

/// The unit a cgroup line belongs to, or None for anything outside a unit.
///
/// A user unit's cgroup path ends in the unit name, so the last `.service`
/// component IS the unit. Taking the LAST is what makes this correct inside
/// nested slices, where earlier components are slices with their own names.
fn unit_of_cgroup(cgroup: &str) -> Option<&str> {
    cgroup.trim().rsplit('/').find(|s| s.ends_with(".service"))
}

/// Notable things a command line says about itself. Derived from the process
/// rather than from a unit file, because the flags that arm this engine live
/// in a drop-in that is deliberately not committed — there is no file in this
/// repo that could tell the truth about arming.
fn tags_of(cmd: &str) -> Vec<String> {
    let mut out = Vec::new();
    let armed = cmd.contains("--enable-orders") && cmd.contains("--yes-trade-live");
    if cmd.contains("arb-trader") {
        out.push(if armed { "ARMED".into() } else { "dry-run".into() });
    }
    if cmd.contains("--take-take") {
        out.push("take-take".into());
    }
    if cmd.contains("--positions-recon-act") {
        out.push("recon act".into());
    }
    if let Some(rest) = cmd.split("--rel-prefix ").nth(1) {
        if let Some(prefix) = rest.split_whitespace().next() {
            out.push(format!("scope {prefix}"));
        }
    }
    out
}

/// Every arbbot process on the machine, keyed by its unit.
///
/// `/proc/<pid>`'s mtime is the process start time, which is why no timestamp
/// has to be parsed out of `systemctl`. Where a unit has several processes
/// (an `ExecStart=/bin/sh -c …` grows a child), the OLDEST is the main one.
pub fn procs_by_unit() -> BTreeMap<String, Proc> {
    let now = now_secs();
    let mut out: BTreeMap<String, Proc> = BTreeMap::new();
    let Ok(rd) = std::fs::read_dir("/proc") else { return out };
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(pid) = name.parse::<u32>() else { continue };
        let Ok(cgroup) = std::fs::read_to_string(format!("/proc/{pid}/cgroup")) else { continue };
        let Some(unit) = unit_of_cgroup(&cgroup) else { continue };
        if !unit.starts_with("arbbot-") {
            continue;
        }
        let started = std::fs::metadata(format!("/proc/{pid}"))
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(now);
        let cmd = std::fs::read_to_string(format!("/proc/{pid}/cmdline"))
            .unwrap_or_default()
            .replace('\0', " ")
            .trim()
            .to_string();
        let p = Proc { pid, up_s: now.saturating_sub(started), cmd };
        out.entry(unit.to_string())
            .and_modify(|cur| {
                if p.up_s > cur.up_s {
                    *cur = p.clone();
                }
            })
            .or_insert(p);
    }
    out
}

/// `systemctl --user … --output=json`, or None if systemd cannot be reached —
/// which is a normal state for this binary run by hand outside a session.
fn systemctl_json(args: &[&str]) -> Option<Value> {
    let out = Command::new("systemctl").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
}

/// unit -> (next elapse, last trigger) in epoch seconds. A timer systemd is
/// not scheduling is ABSENT here, which is exactly what makes the trap
/// visible.
fn timers_json(v: &Value) -> BTreeMap<String, (Option<u64>, Option<u64>)> {
    let mut out = BTreeMap::new();
    // `next`/`last` are microseconds since the epoch; 0 means "never".
    let usec = |v: Option<&Value>| v.and_then(Value::as_u64).filter(|n| *n > 0).map(|n| n / 1_000_000);
    for row in v.as_array().into_iter().flatten() {
        let Some(unit) = row.get("unit").and_then(Value::as_str) else { continue };
        out.insert(unit.to_string(), (usec(row.get("next")), usec(row.get("last"))));
    }
    out
}

/// unit -> enabled | disabled | static. Answers "will this come back after a
/// reboot", which nothing else on this page does.
fn unit_files_json(v: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for row in v.as_array().into_iter().flatten() {
        let Some(unit) = row.get("unit_file").and_then(Value::as_str) else { continue };
        let state = row.get("state").and_then(Value::as_str).unwrap_or("unknown");
        out.insert(unit.to_string(), state.to_string());
    }
    out
}

/// Age in seconds and size in bytes of one file.
fn stat(path: &str) -> Option<(u64, u64)> {
    let m = std::fs::metadata(path).ok()?;
    if !m.is_file() {
        return None;
    }
    let mtime = m.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some((now_secs().saturating_sub(mtime), m.len()))
}

/// The newest `<prefix>…` file in a directory, as (age, bytes). Per-day
/// artifacts are named per day, so the directory's own mtime says only when a
/// file was CREATED — never whether today's is still growing.
fn newest(dir: &str, prefix: &str) -> Option<(u64, u64)> {
    let rd = std::fs::read_dir(dir).ok()?;
    rd.flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
        .filter_map(|e| stat(&e.path().to_string_lossy()))
        .min_by_key(|(age, _)| *age)
}

/// How an age reads against the bound the topology set for it.
///
/// `None` for `max` reports the age and judges nothing: a ledger that has not
/// moved in two days is a quiet market, not a fault, and a bound invented for
/// it would cry wolf every weekend.
fn age_state(age: Option<u64>, max: Option<u64>, expect: Expect) -> &'static str {
    match (age, max, expect) {
        (None, _, Expect::Off) => "off",
        (None, _, Expect::Live) => "down",
        (Some(a), Some(m), Expect::Off) if a <= m => "alive",
        (Some(_), _, Expect::Off) => "off",
        (Some(a), Some(m), Expect::Live) => {
            if a <= m {
                "live"
            } else {
                "stale"
            }
        }
        (Some(_), None, Expect::Live) => "live",
    }
}

/// A timer is live when it is SCHEDULED. Enabled-and-active is not enough: a
/// re-enabled `OnBootSec` timer reports both and never fires again, and it
/// leaves no other trace than the absence of a next elapse.
fn timer_state(next: Option<u64>, now: u64) -> &'static str {
    match next {
        Some(t) if t > now => "live",
        Some(_) => "stale",
        None => "down",
    }
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

pub fn json() -> String {
    let now = now_secs();
    let procs = procs_by_unit();
    let timers = systemctl_json(&["--user", "list-timers", "arbbot*", "--all", "--no-pager", "--output=json"])
        .map(|v| timers_json(&v));
    let files = systemctl_json(&["--user", "list-unit-files", "arbbot*", "--no-pager", "--output=json"])
        .map(|v| unit_files_json(&v));

    let mut warnings: Vec<String> = Vec::new();
    if timers.is_none() || files.is_none() {
        warnings.push(
            "systemctl --user did not answer, so timer schedules and enabled state are unknown \
             on this page. Processes and files are still measured directly."
                .into(),
        );
    }
    let timers = timers.unwrap_or_default();
    let files = files.unwrap_or_default();

    let mut nodes: Vec<Value> = Vec::new();
    let mut declared_units: BTreeSet<&str> = BTreeSet::new();

    for d in NODES {
        if let Some(u) = d.unit {
            declared_units.insert(u);
        }
        let (mut age, mut bytes, mut up_s, mut next_s, mut last_s) = (None, None, None, None, None);
        let mut cmd = String::new();
        let mut tags: Vec<String> = Vec::new();

        let state = match d.probe {
            Probe::Unit => {
                let unit = d.unit.unwrap_or("");
                match procs.get(unit) {
                    Some(p) => {
                        up_s = Some(p.up_s);
                        cmd = p.cmd.clone();
                        tags = tags_of(&p.cmd);
                        if d.expect == Expect::Off {
                            "alive"
                        } else {
                            "live"
                        }
                    }
                    None => {
                        if d.expect == Expect::Off {
                            "off"
                        } else {
                            "down"
                        }
                    }
                }
            }
            Probe::File(p) => {
                let s = stat(p);
                age = s.map(|(a, _)| a);
                bytes = s.map(|(_, b)| b);
                age_state(age, d.max_age_s, d.expect)
            }
            Probe::Newest(dir, prefix) => {
                let s = newest(dir, prefix);
                age = s.map(|(a, _)| a);
                bytes = s.map(|(_, b)| b);
                age_state(age, d.max_age_s, d.expect)
            }
            Probe::Timer => {
                let unit = d.unit.unwrap_or("");
                let (next, last) = timers.get(unit).copied().unwrap_or((None, None));
                next_s = next;
                last_s = last;
                timer_state(next, now)
            }
        };

        // The two drift directions a node can carry on its own face.
        let note = match (state, d.expect) {
            ("alive", Expect::Off) => Some(
                "declared OFF here but it is alive — the diagram is out of date, or this came \
                 back without anyone saying so."
                    .to_string(),
            ),
            ("down", Expect::Live) if matches!(d.probe, Probe::Timer) => Some(
                "no next elapse: systemd is not scheduling this timer, whatever its enabled \
                 state says."
                    .to_string(),
            ),
            _ => None,
        };
        if let Some(n) = &note {
            warnings.push(format!("{}: {n}", d.label));
        }

        nodes.push(json!({
            "id": d.id, "label": d.label, "sub": d.sub,
            "lane": d.lane, "row": d.row, "kind": d.kind,
            "what": d.what, "unit": d.unit, "state": state,
            "expect": if d.expect == Expect::Off { "off" } else { "live" },
            "age_s": age, "bytes": bytes, "up_s": up_s,
            "next_s": next_s, "last_s": last_s,
            "max_age_s": d.max_age_s,
            "enabled": d.unit.and_then(|u| files.get(u)).cloned(),
            "cmd": cmd, "tags": tags,
            "pid": d.unit.and_then(|u| procs.get(u)).map(|p| p.pid),
        }));
    }

    // Edge liveness is its endpoints': an edge cannot be carrying anything if
    // either end of it is not there.
    let state_of: BTreeMap<&str, &str> = NODES
        .iter()
        .zip(nodes.iter())
        .map(|(d, v)| (d.id, v["state"].as_str().unwrap_or("down")))
        .collect();
    let edges: Vec<Value> = EDGES
        .iter()
        .map(|e| {
            let ok = |id: &str| matches!(state_of.get(id).copied(), Some("live" | "alive"));
            json!({ "from": e.from, "to": e.to, "label": e.label, "kind": e.kind,
                    "live": ok(e.from) && ok(e.to) })
        })
        .collect();

    // What is running that this file has never heard of. A diagram that cannot
    // say this is a diagram you cannot trust when it says nothing.
    let undeclared: Vec<Value> = procs
        .iter()
        .filter(|(u, _)| !declared_units.contains(u.as_str()))
        .map(|(u, p)| json!({ "unit": u, "pid": p.pid, "up_s": p.up_s, "cmd": p.cmd }))
        .collect();
    for u in undeclared.iter().filter_map(|v| v["unit"].as_str()) {
        warnings.push(format!("{u} is running and is not on this diagram."));
    }
    for u in &declared_units {
        if !files.is_empty() && !files.contains_key(*u) {
            warnings.push(format!("{u} is on this diagram and is not installed on this machine."));
        }
    }

    let installed: Vec<Value> = files
        .iter()
        .map(|(u, s)| {
            json!({ "unit": u, "enabled": s, "running": procs.contains_key(u),
                    "declared": declared_units.contains(u.as_str()) })
        })
        .collect();

    serde_json::to_string(&json!({
        "lanes": LANES,
        "nodes": nodes,
        "edges": edges,
        "warnings": warnings,
        "undeclared": undeclared,
        "installed": installed,
        "generated_at": now,
    }))
    .unwrap_or_else(|_| "{}".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The topology is the one hand-written thing here, so the one thing that
    /// can rot is a reference into it. An edge naming a node that no longer
    /// exists would draw nothing and say nothing.
    #[test]
    fn every_edge_names_two_declared_nodes() {
        let ids: BTreeSet<&str> = NODES.iter().map(|n| n.id).collect();
        for e in EDGES {
            assert!(ids.contains(e.from), "edge from unknown node {}", e.from);
            assert!(ids.contains(e.to), "edge to unknown node {}", e.to);
            assert_ne!(e.from, e.to, "an edge to itself draws nothing");
        }
    }

    /// The layout is a grid and the client draws where it is told, so two
    /// nodes in one cell is one node invisibly underneath another.
    #[test]
    fn no_two_nodes_share_a_cell() {
        let mut seen = BTreeSet::new();
        for n in NODES {
            assert!(seen.insert((n.lane, n.row)), "{} collides at ({},{})", n.id, n.lane, n.row);
            assert!((n.lane as usize) < LANES.len(), "{} is in no lane", n.id);
        }
    }

    #[test]
    fn ids_are_unique() {
        let ids: BTreeSet<&str> = NODES.iter().map(|n| n.id).collect();
        assert_eq!(ids.len(), NODES.len(), "two nodes share an id");
    }

    /// A node whose liveness IS its unit's must name one, or it silently reads
    /// as permanently down.
    #[test]
    fn a_unit_probed_node_names_a_unit() {
        for n in NODES {
            if matches!(n.probe, Probe::Unit | Probe::Timer) {
                assert!(n.unit.is_some(), "{} probes a unit it does not name", n.id);
            }
        }
    }

    /// The last `.service` component is the unit, inside nested slices too.
    #[test]
    fn a_cgroup_line_yields_its_unit() {
        assert_eq!(
            unit_of_cgroup(
                "0::/user.slice/user-1000.slice/user@1000.service/app.slice/arbbot-trader-m3.service\n"
            ),
            Some("arbbot-trader-m3.service"),
            "the user manager's own unit must not win over the app's"
        );
        assert_eq!(unit_of_cgroup("0::/\n"), None);
        assert_eq!(unit_of_cgroup(""), None);
    }

    /// Arming is readable ONLY from the process: the flags live in an
    /// uncommitted drop-in, so no file in this repo carries them.
    #[test]
    fn arming_is_read_off_the_command_line() {
        let armed = "/rust/target/release/arb-trader --socket data/arbbot.sock --rel-prefix xvus- \
                     --take-take --enable-orders --yes-trade-live --positions-recon-act";
        let tags = tags_of(armed);
        assert!(tags.contains(&"ARMED".to_string()));
        assert!(tags.contains(&"take-take".to_string()));
        assert!(tags.contains(&"scope xvus-".to_string()));

        let dry = "/rust/target/release/arb-trader --socket data/arbbot.sock --out intents.jsonl";
        assert_eq!(tags_of(dry), vec!["dry-run".to_string()]);
    }

    /// ONE of the two flags is not armed, and must never read as armed.
    #[test]
    fn one_arming_flag_alone_is_still_dry_run() {
        assert_eq!(tags_of("arb-trader --enable-orders"), vec!["dry-run".to_string()]);
        assert_eq!(tags_of("arb-trader --yes-trade-live"), vec!["dry-run".to_string()]);
    }

    /// The monotonic-timer trap: `systemctl` reports enabled AND active for a
    /// re-enabled OnBootSec timer that will never fire again. The only trace
    /// is the missing next elapse, so that is what this reads.
    #[test]
    fn a_timer_with_no_next_elapse_is_down() {
        const NOW: u64 = 1_785_530_000;
        assert_eq!(timer_state(None, NOW), "down");
        assert_eq!(timer_state(Some(NOW + 300), NOW), "live");
        assert_eq!(timer_state(Some(NOW - 300), NOW), "stale", "a next elapse in the past fired");
    }

    /// `0` is systemd's "never", and it must not read as an elapse in 1970.
    #[test]
    fn a_zero_timestamp_is_never_not_the_epoch() {
        let v = serde_json::json!([{"unit":"arbbot-x.timer","next":0,"last":0}]);
        assert_eq!(timers_json(&v).get("arbbot-x.timer"), Some(&(None, None)));
        let v = serde_json::json!([{"unit":"arbbot-x.timer","next":1_785_530_851_952_401u64,
                                    "last":1_785_527_251_951_146u64}]);
        assert_eq!(
            timers_json(&v).get("arbbot-x.timer"),
            Some(&(Some(1_785_530_851), Some(1_785_527_251))),
            "microseconds since the epoch, not seconds"
        );
    }

    /// A missing file is DOWN, never fresh. Zero would read as "written this
    /// instant", which is the ambiguity that turned 35 unpriceable ledger
    /// records into a reported $56 of profit.
    #[test]
    fn an_absent_artifact_is_down_not_fresh() {
        assert_eq!(age_state(None, Some(60), Expect::Live), "down");
        assert_eq!(age_state(Some(59), Some(60), Expect::Live), "live");
        assert_eq!(age_state(Some(61), Some(60), Expect::Live), "stale");
    }

    /// An unbounded artifact reports its age and is judged by nothing — a
    /// ledger quiet for two days is a quiet market.
    #[test]
    fn an_unbounded_artifact_never_goes_stale() {
        assert_eq!(age_state(Some(400_000), None, Expect::Live), "live");
    }

    /// The drift direction that is easy to miss: something declared OFF that
    /// is demonstrably still writing. Silence about it is how a component
    /// nobody believes is on becomes one nobody is watching.
    #[test]
    fn a_node_declared_off_that_is_still_writing_is_reported() {
        assert_eq!(age_state(Some(10), Some(300), Expect::Off), "alive");
        assert_eq!(age_state(Some(9_000), Some(300), Expect::Off), "off");
        assert_eq!(age_state(None, Some(300), Expect::Off), "off");
    }

    /// The whole document must be built and serialisable on this machine,
    /// whatever is or is not running on it.
    #[test]
    fn the_document_builds() {
        let v: Value = serde_json::from_str(&json()).expect("valid json");
        assert_eq!(v["nodes"].as_array().map(Vec::len), Some(NODES.len()));
        assert_eq!(v["edges"].as_array().map(Vec::len), Some(EDGES.len()));
        assert!(v["lanes"].is_array());
    }
}
