//! The Now view — what the engine is doing this minute, and what is stopping
//! it from doing more.
//!
//! Every other view here answers a question about the past. This one answers
//! the operator's actual question, which is always some form of "there is an
//! opportunity on the screen; why is nothing happening?" — and the honest
//! answer to that is almost never in the opportunity. It is in a cap.
//!
//! ONE RULE, and everything below follows from it: **the reasons come from the
//! engine, verbatim.** A dashboard that recomputes "would this have passed the
//! class cap" is a second implementation of the gate, and a second
//! implementation disagrees eventually — quietly, and about money. So the
//! numbers on the constraint panel are PARSED OUT OF the engine's own refusal
//! strings (`arb_core::risk::gate`), never recalculated:
//!
//!   `class cap: 331.7+2.5 > 343.00`            -> deployed, need, limit
//!   `topic budget [time-poty-26]: 154+5 > 150` -> topic, deployed, need, limit
//!   `topic [other] gated to util<0.5: at 0.86` -> topic, gate, current util
//!   `global cap: ...` / `per-relationship tail cap: ...` / `insufficient <venue> balance: ...`
//!
//! What this view adds is only arithmetic ON those numbers — the break point:
//! how much has to come free before the thing the engine just refused would
//! fit. That is a subtraction the engine has no reason to do and the operator
//! always has to.
//!
//! Two sources are read that no other view touches:
//!
//!   * the engine's own 60-second `summary()` line, out of the JOURNAL. It is
//!     `println!`ed rather than written to a file, so this is where it lives;
//!     reading it costs one bounded `journalctl` and needs no change to — and
//!     no restart of — the armed engine.
//!   * the tail of the scanner's live opportunity stream, for what is
//!     crossing RIGHT NOW. Only the tail: that file is ~780 MB by evening and
//!     the question is about the last few seconds.
//!
//! What this view CANNOT do, stated plainly because the gap is the interesting
//! part: a skip line is `{"skip":[reason], "ts":…}` and carries no
//! relationship id, so no refusal can be attributed to the pair that provoked
//! it. Topic-scoped gates are attributable — the topic is in the string, and
//! `arb_core::risk::topic_of` buckets a pair the same way the gate did — but
//! `class cap` and `global cap` name no scope and apply to everything. Rows
//! say which of the two they are getting.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arb_core::clock::now_secs;
use arb_core::risk::{topic_of, TopicIn};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::Args;

/// How much of each tail to read. The intents tape is the window this view
/// reports over; the opportunity stream is only ever "what is crossing now".
const INTENTS_TAIL: u64 = 6 << 20;
const OPPS_TAIL: u64 = 4 << 20;
const LEDGER_TAIL: u64 = 256 << 10;
/// "Now" in seconds. The byte tails above bound the READ; this bounds the
/// CLAIM. Without it the window is however much history happened to fit in
/// 6 MB — a day, on a quiet engine — and "the class cap refused 85,000 times"
/// stops being a rate anybody can act on.
const WINDOW_S: f64 = 900.0;
/// Newest events shown verbatim. A feed, not a database.
const RECENT: usize = 40;

/// The last `bytes` of a file as parsed JSON lines.
///
/// The cut lands mid-line, so the first fragment is dropped — a torn record
/// parsed leniently is how a half-read price becomes a number.
fn tail_json(path: &str, bytes: u64) -> Vec<Value> {
    let Ok(m) = std::fs::metadata(path) else { return Vec::new() };
    let from = m.len().saturating_sub(bytes);
    let Ok(mut f) = std::fs::File::open(path) else { return Vec::new() };
    use std::io::{Read, Seek, SeekFrom};
    if f.seek(SeekFrom::Start(from)).is_err() {
        return Vec::new();
    }
    let mut buf = String::new();
    if f.take(bytes + (1 << 20)).read_to_string(&mut buf).is_err() {
        return Vec::new();
    }
    let mut lines = buf.lines();
    if from > 0 {
        lines.next();
    }
    lines.filter_map(|l| serde_json::from_str(l).ok()).collect()
}

fn f(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Constraints — parsed from the engine's refusals, never recomputed
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Gate {
    gate: String,
    scope: Option<String>,
    hits: u64,
    deployed: Option<f64>,
    need: Option<f64>,
    limit: Option<f64>,
    /// For the low-utilisation gate only: where utilisation actually is.
    at: Option<f64>,
    last_ts: f64,
    text: String,
}

/// Read `a+b > c` out of the tail of a reason, ignoring any parenthetical the
/// gate appended (`(overflow needs apr>=…)`).
fn triple(rest: &str) -> (Option<f64>, Option<f64>, Option<f64>) {
    let head = rest.split(" (").next().unwrap_or(rest);
    let (lhs, limit) = match head.split_once('>') {
        Some((l, r)) => (l, r.trim().parse::<f64>().ok()),
        None => (head, None),
    };
    let (deployed, need) = match lhs.trim().split_once('+') {
        Some((d, n)) => (d.trim().parse().ok(), n.trim().parse().ok()),
        None => (None, None),
    };
    (deployed, need, limit)
}

/// One refusal string -> (gate name, scope, numbers). Unrecognised reasons are
/// kept under their own leading phrase rather than dropped: a gate this parser
/// has never seen must still be visible, and visibly unparsed.
fn classify(reason: &str) -> Gate {
    let mut g = Gate { text: reason.to_string(), hits: 1, ..Default::default() };
    if let Some(rest) = reason.strip_prefix("class cap: ") {
        g.gate = "class cap".into();
        (g.deployed, g.need, g.limit) = triple(rest);
    } else if let Some(rest) = reason.strip_prefix("global cap: ") {
        g.gate = "global cap".into();
        (g.deployed, g.need, g.limit) = triple(rest);
    } else if let Some(rest) = reason.strip_prefix("topic budget [") {
        g.gate = "topic budget".into();
        if let Some((topic, nums)) = rest.split_once("]: ") {
            g.scope = Some(topic.to_string());
            (g.deployed, g.need, g.limit) = triple(nums);
        }
    } else if let Some(rest) = reason.strip_prefix("topic [") {
        g.gate = "low-utilisation gate".into();
        if let Some((topic, nums)) = rest.split_once("] gated to util<") {
            g.scope = Some(topic.to_string());
            if let Some((lim, at)) = nums.split_once(": at ") {
                g.limit = lim.trim().parse().ok();
                g.at = at.trim().parse().ok();
            }
        }
    } else if let Some(rest) = reason.strip_prefix("insufficient ") {
        g.gate = "venue cash".into();
        if let Some((venue, nums)) = rest.split_once(" balance: ") {
            g.scope = Some(venue.to_string());
            if let Some((need, avail)) = nums.split_once('>') {
                g.need = need.trim().parse().ok();
                g.limit = avail.trim().parse().ok();
                g.deployed = Some(0.0);
            }
        }
    } else if reason.starts_with("per-relationship tail cap") {
        g.gate = "per-relationship tail cap".into();
    } else {
        // A gate this parser does not know must still GROUP, or one row per
        // price turns a single repeating condition into a wall of rows. The
        // numbers are what vary; the words are the condition. The newest
        // reason survives verbatim in `text`.
        g.gate = reason
            .split(':')
            .next()
            .unwrap_or(reason)
            .split_whitespace()
            .map(|w| if w.chars().any(|c| c.is_ascii_digit()) { "…" } else { w })
            .collect::<Vec<_>>()
            .join(" ");
    }
    g
}

/// What has to come free before the refused order would fit.
///
/// This is the only number on the panel the engine did not produce, and it is
/// a subtraction of two it did. Negative headroom is not clamped: a topic $4
/// past its budget should read as $4 past it.
fn break_point(g: &Gate, class_cap: Option<f64>) -> (Option<f64>, Option<f64>) {
    if g.gate == "low-utilisation gate" {
        // Utilisation is a fraction of the class cap, so the dollars that must
        // come off the book to reopen the gate need that cap to be expressed.
        let free = match (g.at, g.limit, class_cap) {
            (Some(at), Some(lim), Some(cap)) if at >= lim => Some((at - lim) * cap),
            _ => None,
        };
        return (None, free);
    }
    let headroom = match (g.limit, g.deployed) {
        (Some(l), Some(d)) => Some(l - d),
        _ => None,
    };
    let free = match (headroom, g.need) {
        (Some(h), Some(n)) if n > h => Some(n - h),
        _ => None,
    };
    (headroom, free)
}

/// The best crossing seen on one relationship in the opportunity tail.
#[derive(Default)]
struct Opp {
    edge: f64,
    total: f64,
    size: String,
    tranche: String,
    n: u64,
    last: f64,
}

// ---------------------------------------------------------------------------
// The engine's own summary, out of the journal
// ---------------------------------------------------------------------------

/// The engine this view is about: the ARMED `arb-trader` if one is running,
/// else whichever `arb-trader` is.
///
/// Detected rather than configured, for the same reason the Architecture view
/// detects it — the flags that arm an engine live in a drop-in that is not in
/// this repo, so a configured unit name is a guess that goes stale the first
/// time a slice is renamed or a second one is armed.
fn armed_engine() -> Option<(String, crate::architecture::Proc)> {
    let mut fallback = None;
    for (unit, p) in crate::architecture::procs_by_unit() {
        if !p.cmd.contains("arb-trader") {
            continue;
        }
        if p.cmd.contains("--enable-orders") && p.cmd.contains("--yes-trade-live") {
            return Some((unit, p));
        }
        if fallback.is_none() {
            fallback = Some((unit, p));
        }
    }
    fallback
}

/// The intents file an engine is actually writing, read off its command line.
/// The armed slice and the shadow write different files and only one of them
/// is this view's subject.
fn out_path(cmd: &str) -> Option<String> {
    cmd.split("--out ").nth(1)?.split_whitespace().next().map(str::to_string)
}

/// The newest `summary()` line a unit printed, and how old it is.
///
/// Dated from `elapsed_s` (the engine's own uptime at the moment it printed)
/// plus the process start time, so no journal timestamp has to be parsed and
/// the age is the engine's clock rather than journald's.
fn engine_summary(unit: &str, up_s: u64) -> (Option<Value>, Option<u64>, Option<String>) {
    let started = now_secs().saturating_sub(up_s);
    let out = std::process::Command::new("journalctl")
        .args(["--user", "-u", unit, "--since", "-15min", "-n", "80", "-o", "cat", "--no-pager"])
        .output();
    let Ok(out) = out else {
        return (None, None, Some("journalctl is not reachable from here".into()));
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let newest = text
        .lines()
        .rev()
        .filter(|l| l.starts_with('{'))
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        // `elapsed_s` is what makes a line a summary rather than some other
        // JSON the engine happened to print.
        .find(|v| v.get("elapsed_s").is_some());
    match newest {
        Some(v) => {
            let age = f(v.get("elapsed_s"))
                .map(|e| now_secs().saturating_sub(started + e as u64));
            (Some(v), age, None)
        }
        None => (
            None,
            None,
            Some("no summary in the last 15 minutes of the journal".into()),
        ),
    }
}

/// The gauges worth a headline, in the order an operator reads them. Anything
/// not named here is still returned under `all`, because a 75-gauge summary is
/// exactly the thing you want in full when something is wrong.
const HEADLINE: [(&str, &str); 13] = [
    ("risk_allowed", "orders the risk gate passed"),
    ("risk_rejected", "orders the risk gate refused"),
    // The capital panel's `gate_on` is derived from the snapshot file, which
    // dates itself; this is the engine's own answer, and it is the only thing
    // that separates a poll which has not answered YET from one that never
    // will. Read it with `summary_age_s` — it is as old as the line it came in.
    ("balances_source", "what the cash gate is spending against"),
    ("take_take_found", "immediately-executable crossings seen"),
    ("take_take_fired", "…of those, taken"),
    ("take_take_gated", "…of those, refused by a gate"),
    ("take_take_bar_apr", "the APR bar take-take must beat"),
    ("maker_apr_bar", "the APR bar a maker quote must beat"),
    ("order_acks", "orders the venues acknowledged"),
    ("fills", "fills seen"),
    ("hedges_pending", "hedge obligations still open"),
    ("hedges_naked", "legs that ended up naked"),
    ("unwind_actionable", "positions the exit logic would act on"),
];

// ---------------------------------------------------------------------------
// Topic budgets
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct TopicsDoc {
    #[serde(default)]
    topics: Vec<TopicIn>,
    #[serde(default)]
    default_topic_budget: Option<serde_yaml::Value>,
}

#[derive(Deserialize, Default)]
struct CapsDoc {
    bankroll_usd: Option<serde_yaml::Value>,
    per_class_cap: Option<serde_yaml::Value>,
}

fn yaml_num(v: &Option<serde_yaml::Value>) -> Option<f64> {
    match v.as_ref()? {
        serde_yaml::Value::Number(n) => n.as_f64(),
        serde_yaml::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Capital, derived rather than typed
// ---------------------------------------------------------------------------

/// Past this, `data/exec/marks.json` is not a picture of now.
///
/// The armed engine rewrites it on a book event, coalesced to at most one write
/// per second and at LEAST one per `engine::MarksOut::max_idle_s`, which is
/// 120s. So an older file does not mean the book went quiet — it means the
/// writer is dead or wedged, and that is the one failure mode that turns the
/// deployed figure below from a lag into a lie.
const MARKS_STALE_S: u64 = 120;

/// Stale is a THIRD state, not a pass, and an age nobody could establish is
/// UNKNOWN rather than fresh. Same rule the books page applies to its venue
/// snapshots, and the same exclusive boundary.
///
/// There is deliberately no constant here for the engine's cash snapshot. That
/// bound is the gate's own `risk::BALANCE_MAX_AGE`, and the engine PUBLISHES it
/// in the file it writes (`max_age_s`) so a reader does not re-transcribe 180
/// and drift from it the first time the expiry moves — see
/// [`balances_snapshot`]. A source that publishes no bound therefore has an
/// UNKNOWN staleness rather than a defaulted one, which is why the measured
/// side carries an optional bound and not a constant.
fn stale(age_s: Option<u64>, past_s: u64) -> Option<bool> {
    age_s.map(|x| x > past_s)
}

/// Capital DEPLOYED, out of the engine's own marks: what flattening the whole
/// book would return, and each separate way an open record can be missing from
/// that figure.
///
/// The default is what an absent marks file yields: UNKNOWN on every count
/// except `unpriced_rows`, where zero rows really is zero rows that failed to
/// price.
#[derive(Default)]
struct Deployed {
    /// Σ `liq_value_usd` over the published rows.
    share_value_usd: Option<f64>,
    /// Rows that WERE published but carry no `liq_value_usd`.
    unpriced_rows: u64,
    /// `totals.n_open - positions.len()`: open records that produced no row at
    /// all. NOT the single-leg riders alone — see [`deployed`].
    unmarked_records: Option<i64>,
    /// `totals.unpriced_positions`: of those, the two-leg baskets.
    unpriced_baskets: Option<u64>,
    /// ...and the remainder, which is the single-leg riders.
    single_leg_records: Option<i64>,
}

/// Capital DEPLOYED, out of the engine's own marks.
///
/// [`Deployed::share_value_usd`] is Σ `liq_value_usd`, which is the basket sold
/// at the bid and bought back at the ask NET OF BOTH EXIT FEES
/// (`marks::compute_row`) — the recoverable figure, not a mid. Deliberately not
/// the shortcut `totals.cost_usd + totals.mark_pnl_usd`: `marks::build` skips a
/// row it could not price when it accumulates the mark but adds EVERY row's
/// cost, so that sum silently carries an unpriceable row at full cost —
/// overstating what is recoverable exactly where the book is least knowable. It
/// happens to agree today only because all 58 rows priced.
///
/// The rest is the accounting for what is NOT in that sum, and it takes three
/// numbers rather than one. `marks::build` states the identity itself:
///
///   `n_open - positions.len() - unpriced_positions == single-leg records`
///
/// RETRACTION. The first version of this function published `n_open -
/// positions.len()` alone and the panel called it single-leg riders. It is not.
/// A two-leg basket `marks::build` could not price — no cost basis derivable
/// from its legs, or a leg missing on one venue — is skipped with no row emitted
/// AND counted in `unpriced_positions`, so it lands inside that subtraction too.
/// Reporting the whole gap as directional riders attributes real unmarkable
/// baskets, which is money sitting at a venue, to positions that were never part
/// of one. Both halves are published now and the single-leg count is the
/// remainder, so the panel can stop guessing which it is looking at.
///
/// [`Deployed::unpriced_rows`] is a fourth and different thing: a row that WAS
/// published but whose `liq_value_usd` is null — no bid to liquidate into, or a
/// market the recorder does not carry. Its cost is still inside
/// `totals.cost_usd`, so the recoverable sum beside it is a floor.
///
/// `None` rather than `0.0` when there is no marks file. A $0.00 under
/// "recoverable" reads as "the book is flat", which is a very different claim
/// from "the process that writes this file is not running".
fn deployed(marks: &Value) -> Deployed {
    let Some(rows) = marks.get("positions").and_then(Value::as_array) else {
        return Deployed::default();
    };
    let (mut value, mut unpriced_rows) = (0.0, 0u64);
    for p in rows {
        match f(p.get("liq_value_usd")) {
            Some(v) => value += v,
            None => unpriced_rows += 1,
        }
    }
    let totals = marks.get("totals");
    let unmarked_records = totals
        .and_then(|t| t.get("n_open"))
        .and_then(Value::as_u64)
        .map(|n| n as i64 - rows.len() as i64);
    let unpriced_baskets =
        totals.and_then(|t| t.get("unpriced_positions")).and_then(Value::as_u64);
    Deployed {
        share_value_usd: Some(value),
        unpriced_rows,
        unmarked_records,
        unpriced_baskets,
        single_leg_records: unmarked_records.zip(unpriced_baskets).map(|(u, b)| u - b as i64),
    }
}

/// Per-venue cash out of the snapshot the ARMED engine publishes: the figures,
/// the moment the venues answered, and the age past which the engine itself
/// stops believing them.
///
/// THE CONTRACT, read out of `arb-trader`'s `publish_balances` rather than
/// assumed: `data/exec/balances.json`, holding
/// `{"at": <unix seconds>, "max_age_s": <seconds>, "balances": {"<venue>":
/// "<figure>"}}`. The figures are STRINGS — they are whatever Kalshi's
/// `balance_dollars` and PM-US's `buyingPower` held on the wire, and the engine
/// installs them into its gate unparsed — which is why [`f`] takes both. The
/// venue names are `crate::VENUES`, the keys every other view in this binary
/// already reads.
///
/// `max_age_s` IS READ, NOT RE-TRANSCRIBED. It is the gate's own
/// `risk::BALANCE_MAX_AGE`, and the writer publishes it precisely so a reader
/// can say "the engine has stopped believing this" without copying 180 into a
/// second file and drifting from it. A snapshot carrying no bound has an
/// unknown staleness here rather than a guessed one.
///
/// `at` AND NOTHING ELSE DATES THE READING — deliberately no fall back to the
/// file's mtime. `publish_balances` is called only on a cycle that produced a
/// usable reading, so for THIS writer the two agree; but a file with no `at` is
/// not this writer's file, and dating an unknown writer off its mtime is the
/// manufactured confidence this panel exists to remove.
///
/// A document without a `balances` object, or one carrying no venue this binary
/// knows, measures NOTHING: the caller says the measured side is unavailable
/// rather than falling through to silence.
fn balances_snapshot(text: &str) -> (BTreeMap<String, f64>, Option<f64>, Option<u64>) {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Default::default();
    };
    let Some(inner) = v.get("balances") else {
        return Default::default();
    };
    let cash = crate::VENUES
        .iter()
        .filter_map(|name| Some(((*name).to_string(), f(inner.get(*name))?)))
        .collect();
    (cash, f(v.get("at")), v.get("max_age_s").and_then(Value::as_u64))
}

/// What the armed engine's cash gate is spending against RIGHT NOW, decided
/// from the snapshot's own two fields.
///
/// This is the fact that stops the measured column being read as a second
/// opinion. When the reading is inside its published bound the gate IS spending
/// against it, so the measured column and the engine's working figure are one
/// number and there is nothing to compare. When it is past that bound the gate
/// has no figure at all — `RiskView::spendable` yields no pairs, every venue
/// looks up $0, and the `insufficient <venue> balance` refusal fires on
/// everything. Those are opposite states behind the same displayed number.
///
/// Off the FILE rather than off the engine's `balances_source` gauge, which is
/// the more direct answer and the wrong one to build on here: that gauge
/// reaches this page inside a `summary()` line that can be up to fifteen
/// minutes old, so it says what the gate was on, while the snapshot dates
/// itself and says what it is on. The gauge is on the page beside it (it is in
/// [`HEADLINE`]) and it is what resolves the one case this cannot: with no file
/// at all, a poll that has not answered YET (the gate is still spending the
/// `--balance` seed) and a poll that has never once answered (the gate is
/// refusing) look identical from out here.
fn gate_state(
    armed: bool,
    path: &str,
    age_s: Option<u64>,
    bound_s: Option<u64>,
) -> (&'static str, String) {
    if !armed {
        return (
            "none",
            "no armed engine is polling a venue, so nothing on this panel is a figure a risk \
             gate is spending against."
                .into(),
        );
    }
    match (age_s, bound_s) {
        (Some(age), Some(bound)) if age <= bound => (
            "live",
            format!(
                "the gate is spending against the MEASURED column: {path} is {age}s old \
                 against the {bound}s bound the engine publishes for itself, so the venues' \
                 own figure is what authorises every order."
            ),
        ),
        (Some(age), Some(bound)) => (
            "stale",
            format!(
                "the gate is REFUSING EVERY ORDER: {path} is {age}s old, past the {bound}s \
                 bound the engine publishes for itself, and only a successful poll rewrites \
                 it. Past that bound the gate holds no cash at all — every venue reads $0 \
                 and refuses on `insufficient <venue> balance`."
            ),
        ),
        (Some(_), None) => (
            "unknown",
            format!(
                "{path} publishes no max_age_s, so nothing out here can say whether the \
                 engine still believes its own reading. Read `balances_source` in the gauges \
                 above."
            ),
        ),
        (None, _) => (
            "unknown",
            format!(
                "{path} is not there. Either the poll has not answered yet — the gate is \
                 still spending the --balance seed — or it has never answered, in which case \
                 the gate is refusing everything. An absent file cannot tell those apart; \
                 `balances_source` in the gauges above can."
            ),
        ),
    }
}

/// The SPENDABLE PM-US cash in a `pmus_balances.json`.
///
/// `buyingPower`, NEVER `currentBalance`. PM-US reports
/// `currentBalance = buyingPower + marginRequirement` (live: 653.15805 =
/// 329.29805 + 323.86), and the $1.00 it withholds per short contract — the
/// whole of `marginRequirement` — is ALREADY inside each position's cost basis,
/// so it is already inside the deployed figure this sits beside. Taking
/// `currentBalance` here would count that $323.86 of collateral twice, which is
/// the exact bug the 2026-08-14 accounting audit found. `arb_ledger::pmus`
/// documents the convention, and going through its `Balances` means the only
/// accessor this file can reach for is the spendable one.
///
/// Takes the TEXT rather than the path so the trap above is pinned by a test
/// against the live file's own numbers.
fn spendable_pmus(text: &str) -> Option<f64> {
    serde_json::from_str::<arb_ledger::pmus::Balances>(text)
        .ok()?
        .buying_power_str()
        .parse()
        .ok()
}

/// The per-venue cash a `--balance` command line declares, read off `/proc`.
///
/// Whether the process it came from is the one a risk gate is actually spending
/// against is [`engine_cash`]'s question and not this parser's — the fallback
/// that makes it a question is documented there.
///
/// Off `/proc` and not off the unit file, for the same reason `armed_engine`
/// detects rather than configures: the tracked unit says `--balance
/// kalshi=340.09` and the drop-in that actually arms the engine overrides it
/// with `320.43`, so a figure read out of this repo is a figure nobody is
/// trading against.
///
/// RETRACTION (#77 landed). This doc used to say the constant is "what the risk
/// gate is spending against" and that it "is never decremented as capital
/// deploys" — audit C13. That was true of every armed run before #77 and is
/// true of NO armed run after it. `--balance` is now a SEED: `RiskView` holds
/// it as `Cash::Seed`, an armed engine's balance poll replaces it with a venue
/// reading inside one round trip, and the seed EXPIRES on the same
/// `risk::BALANCE_MAX_AGE` clock as a reading, so an armed run whose poll never
/// answers stops trading rather than trading the constant forever. It is what
/// the gate spends against only while `balances_source` reads `seed` (armed,
/// first cycle outstanding) or `declared` (a run that will never poll — bench,
/// replay, the unarmed shadow).
///
/// What has NOT changed: this is not a measurement of venue cash, and it must
/// never be added to the deployed figure. It is a number somebody typed, and on
/// an armed run the shares already counted as deployed were bought with some of
/// the very cash it still names.
fn engine_balances(cmd: &str) -> Vec<(String, f64)> {
    cmd.split("--balance ")
        .skip(1)
        .filter_map(|s| s.split_whitespace().next())
        .filter_map(|kv| {
            let (venue, usd) = kv.split_once('=')?;
            Some((venue.to_string(), usd.parse().ok()?))
        })
        .collect()
}

/// The seed column: whose `--balance` constants it holds, and whether the
/// process they were read off can actually trade.
///
/// [`armed_engine`] falls back to ANY `arb-trader` when none is armed. That
/// fallback is right for the gauges — a shadow's own summary is still worth
/// reading — and it is a TRAP for money. This host runs an unarmed shadow whose
/// `--balance kalshi=340.09` is within $0.0004 of this dashboard's own
/// `--kalshi-balance 340.0896`, so with the armed slice stopped a column headed
/// "armed engine" would quietly fill with the shadow's constants, the one gap
/// on this panel that can genuinely fail would collapse to ~$0, and a stopped
/// engine would render as an all-clear.
///
/// So the column is filled ONLY off an armed process, it always names the unit
/// it was read from, and when nothing is armed it is EMPTY and says why. A panel
/// that cannot tell armed from shadow must not print a heading claiming it can.
fn engine_cash(unit: &str, armed: bool, cmd: Option<&str>) -> (Vec<(String, f64)>, String) {
    match cmd {
        Some(c) if armed => (engine_balances(c), format!("{unit} · --balance seed, ARMED")),
        Some(_) => (
            Vec::new(),
            format!(
                "{unit} is running but is NOT armed. Its --balance constants are not what any \
                 risk gate is spending against, so this column is empty rather than showing a \
                 shadow's numbers under an armed heading."
            ),
        ),
        None => (Vec::new(), "no arb-trader is running".into()),
    }
}

/// The MEASURED side of one venue's cash: a figure some process actually read
/// off the venue, where it came from, how old it is, the age past which it stops
/// being evidence about now, and WHAT THAT AGE MEANS.
///
/// The last two are per-source and not per-panel, which is the whole reason they
/// live here. The two measured sources this binary can reach age for completely
/// different reasons: one is the reading a risk gate is holding, which the gate
/// abandons at a bound it publishes; the other is a file a person fetched by
/// hand that no gate has ever read. Printing one sentence over both makes one of
/// them false — see [`derived_capital`], which fills these in.
struct Measured {
    usd: Option<f64>,
    from: String,
    age_s: Option<u64>,
    /// `None` where the source publishes no bound; staleness is then UNKNOWN.
    stale_past_s: Option<u64>,
    /// What it means that THIS source has gone stale. Empty where there is no
    /// figure to go stale.
    stale_means: String,
}

/// One venue's cash, beside every other figure that claims to know it, with the
/// one difference between them that means a single thing.
///
/// `dash_diff_usd` IS THAT DIFFERENCE: the `--balance` seed against this
/// dashboard's `--kalshi-balance`. Two numbers a person typed, in two unit
/// files, for the same quantity, either editable alone — 320.43 against
/// 340.0896 today. Nothing moves either of them, so a gap is a typo or a stale
/// edit and can be nothing else.
///
/// `venue_diff_usd` IS GONE, and this is the retraction. It published
/// `--balance` minus the measured figure and the page painted any non-zero as a
/// warning. It never meant one thing. BEFORE #77 `--balance` was a launch-time
/// constant nothing decremented, so the gap was CAPITAL DEPLOYED SINCE LAUNCH —
/// the healthiest number on the panel, rendered as a fault. AFTER #77 it is
/// worse, because the two sides are no longer even about the same instant:
/// while `balances_source` reads `live` the gate is spending against the
/// measured column ITSELF (`data/exec/balances.json` is that reading, published)
/// so the honest gap is structurally zero and a subtraction against the seed is
/// deployment plus whatever the seed was mistyped by; while it reads
/// `seed`/`declared` the gate is on the constant and the measured figure is a
/// number no gate consulted. A constant and a measurement are not two opinions
/// about one instant, and subtracting them implies they should match.
///
/// What replaces it is not a number: `gate_on` on the block says which of the
/// two columns the gate is actually spending against, and `venue_stale_means`
/// on the row says what this source's age costs. The measured figure, its age
/// and its published bound all still show, because all of that is true.
fn cash_row(venue: &str, engine: Option<f64>, dash: Option<f64>, m: &Measured) -> Value {
    json!({
        "venue": venue,
        "engine_usd": engine,
        "dash_usd": dash,
        "dash_diff_usd": engine.zip(dash).map(|(e, x)| e - x),
        "venue_usd": m.usd,
        "venue_from": m.from,
        "venue_age_s": m.age_s,
        "venue_stale_past_s": m.stale_past_s,
        "venue_stale": m.stale_past_s.and_then(|p| stale(m.age_s, p)),
        "venue_stale_means": m.stale_means,
    })
}

/// The capital block's `derived` sibling: everything on this panel that was
/// measured rather than typed, published beside the constants it exists to
/// check.
///
/// DEPLOYED is genuinely live: the engine marks the book against its own feed
/// and rewrites `marks.json` at book-event rate. REMAINING is only ever as live
/// as whatever last read a venue, and nothing here pretends otherwise — arb-dash
/// holds no credentials and constructs no gateway, so every cash figure it can
/// reach is either a constant somebody typed into a unit file or a file some
/// other process wrote. Deriving "capital remaining" from those and serving it
/// as a measurement would reproduce, one level up, exactly the tautology the
/// books page carries: a new number that cannot disagree with the thing it was
/// built to check. So they are published SIDE BY SIDE, each with its provenance
/// and its age.
///
/// AND THE GAPS BETWEEN THEM ARE NOT ALL OUTPUT — the earlier version of this
/// doc said they were, and that is retracted here. Exactly one of them can fail
/// on its own: seed against dashboard constant, two typed numbers. The gap
/// between a constant and a measurement is capital deployed since the constant
/// was typed, which is what a working engine looks like. [`cash_row`] carries
/// that argument in full and is where the second gap was deleted.
///
/// Deployed is not attributed per venue, and cannot be from this file: a marked
/// row is a two-leg BASKET priced as a unit (`marks::compute_row` sums the
/// Kalshi bid and the PM-US 1−ask into one `liq_value_usd`), so there is no
/// per-leg money in `marks.json` to split. Splitting it would need a change to
/// the engine's writer, which is an order-path crate.
///
/// The engine's identity arrives as three arguments rather than being read off
/// `/proc` in here, so the money emission is exercised by tests instead of by
/// whatever happens to be running on the box.
fn derived_capital(a: &Args, marks: &Value, unit: &str, armed: bool, cmd: Option<&str>) -> Value {
    let d = deployed(marks);
    let (engine_bal, engine_from) = engine_cash(unit, armed, cmd);

    // The measured side, best source first. `data/exec/balances.json` is the
    // armed engine's balance poll publishing what the venues just told it, on
    // every cycle that produced a usable reading — see `balances_snapshot` for
    // the contract and `gate_state` for what its age costs. `data/venue/` is
    // fetched BY HAND and has had no writer since 2026-07-27, so on its own it
    // is a three-week-old number and is served as one.
    let snap_path = format!("{}/exec/balances.json", a.data_dir);
    let snap_text = std::fs::read_to_string(&snap_path).ok();
    let (snap, snap_at, snap_bound) =
        snap_text.as_deref().map(balances_snapshot).unwrap_or_default();
    let snap_age = snap_at.map(|t| (now_secs() as f64 - t).max(0.0) as u64);
    let (gate_on, gate_why) = gate_state(armed, &snap_path, snap_age, snap_bound);
    let unmeasured = match snap_text {
        None => format!(
            "no measured figure: {snap_path} does not exist, and this process holds no \
             credentials to read a venue itself"
        ),
        Some(_) => format!("{snap_path} carries no figure for this venue"),
    };

    let pmus_path = format!("{}/pmus_balances.json", a.pmus_dir);
    let pmus_hand = std::fs::read_to_string(&pmus_path).ok().as_deref().and_then(spendable_pmus);
    let measured = |venue: &str| -> Measured {
        if let Some(usd) = snap.get(venue) {
            return Measured {
                usd: Some(*usd),
                from: format!(
                    "{snap_path} · the armed engine's balance poll, dated by its own `at`"
                ),
                age_s: snap_age,
                stale_past_s: snap_bound,
                // TRUE OF THIS SOURCE AND OF NO OTHER. This IS the reading the
                // gate installed, and the bound is the gate's own, published in
                // the same file: past it `RiskView::spendable` returns no pairs
                // at all, so every venue looks up $0 and `insufficient <venue>
                // balance` refuses everything.
                stale_means: "the engine has stopped spending against it: this is the reading \
                              its gate installed, and past the bound the engine publishes here \
                              the gate holds no cash at all — every venue reads $0 and refuses \
                              on `insufficient <venue> balance`"
                    .into(),
            };
        }
        // PM-US only, and only as a fallback: the hand-fetched snapshot the
        // books page reconciles against. It IS a real venue read, and it was the
        // only one this binary had until #77, so it is shown — with its true
        // age (the live file was last fetched 2026-07-27) and with a sentence
        // that does NOT borrow the row above's alarm. No gate ever read it.
        match (venue, pmus_hand) {
            ("polymarket_us", Some(usd)) => Measured {
                usd: Some(usd),
                from: format!(
                    "{pmus_path} · buyingPower, fetched by hand — nothing in this repo writes it"
                ),
                age_s: crate::endpoints::age_secs(&pmus_path),
                // The books page's hour, because this is the books page's file.
                stale_past_s: Some(crate::endpoints::books::SNAPSHOT_STALE_S),
                stale_means: "just old, and nothing changed behaviour when it aged: no risk \
                              gate has ever read this file. It is the books page's hand-fetched \
                              snapshot, not a figure any engine is spending against"
                    .into(),
            },
            _ => Measured {
                usd: None,
                from: unmeasured.clone(),
                age_s: None,
                stale_past_s: None,
                stale_means: String::new(),
            },
        }
    };

    // Only venues something is actually known about. A row of nulls would
    // suggest a venue is configured when it is not.
    let dash_kalshi: Option<f64> = a.kalshi_balance.as_ref().and_then(|s| s.parse().ok());
    let mut venues: BTreeSet<String> = engine_bal.iter().map(|(v, _)| v.clone()).collect();
    venues.extend(snap.keys().cloned());
    if dash_kalshi.is_some() {
        venues.insert("kalshi".into());
    }
    if pmus_hand.is_some() {
        venues.insert("polymarket_us".into());
    }
    let cash: Vec<Value> = venues
        .iter()
        .map(|v| {
            let engine = engine_bal.iter().find(|(n, _)| n == v).map(|(_, u)| *u);
            // `--kalshi-balance` is this process's OWN startup constant and
            // there is no equivalent flag for any other venue, so no other row
            // has a dashboard column. It is never a measurement: putting it in
            // the measured column would compare a hand-typed number to itself.
            let dash = if v == "kalshi" { dash_kalshi } else { None };
            cash_row(v, engine, dash, &measured(v))
        })
        .collect();

    json!({
        "share_value_usd": d.share_value_usd,
        "unpriced_rows": d.unpriced_rows,
        "unmarked_records": d.unmarked_records,
        "unpriced_baskets": d.unpriced_baskets,
        "single_leg_records": d.single_leg_records,
        "engine_from": engine_from,
        "gate_on": gate_on,
        "gate_why": gate_why,
        "cash": cash })
}

// ---------------------------------------------------------------------------

pub fn json(a: &Args) -> String {
    let now = now_secs() as f64;

    let caps: CapsDoc = std::fs::read_to_string(&a.exec_config)
        .ok()
        .and_then(|t| serde_yaml::from_str(&t).ok())
        .unwrap_or_default();
    let bankroll = yaml_num(&caps.bankroll_usd);
    let per_class = yaml_num(&caps.per_class_cap);
    let class_cap = match (bankroll, per_class) {
        (Some(b), Some(p)) => Some(b * p),
        _ => None,
    };

    let topics: TopicsDoc = std::fs::read_to_string(&a.topics_config)
        .ok()
        .and_then(|t| serde_yaml::from_str(&t).ok())
        .unwrap_or_default();

    // ---- the armed engine's own report -----------------------------------
    let engine = armed_engine();
    let unit = engine.as_ref().map(|(u, _)| u.clone()).unwrap_or_default();
    let proc = engine.as_ref().map(|(_, p)| p);
    let armed = proc
        .map(|p| p.cmd.contains("--enable-orders") && p.cmd.contains("--yes-trade-live"))
        .unwrap_or(false);
    let (summary, summary_age, summary_err) = match proc {
        Some(p) => engine_summary(&unit, p.up_s),
        None => (None, None, Some("no arb-trader is running".into())),
    };
    let gauges: Vec<Value> = HEADLINE
        .iter()
        .filter_map(|(k, what)| {
            let v = summary.as_ref()?.get(*k)?;
            Some(json!({ "key": k, "what": what, "value": v }))
        })
        .collect();

    // ---- the window ------------------------------------------------------
    let intents_path = proc
        .and_then(|p| out_path(&p.cmd))
        .unwrap_or_else(|| a.intents_path.clone());
    let all = tail_json(&intents_path, INTENTS_TAIL);
    let lines: Vec<&Value> = all
        .iter()
        .filter(|l| f(l.get("ts")).map(|t| t >= now - WINDOW_S).unwrap_or(false))
        .collect();
    let first_ts = lines.iter().filter_map(|l| f(l.get("ts"))).fold(f64::MAX, f64::min);
    // The span actually COVERED, which is the window unless the tail was too
    // short to reach back that far. Reporting the intent rather than the
    // reality would overstate every rate on the page.
    let window_s = if first_ts < f64::MAX { now - first_ts } else { 0.0 };
    let truncated = all.len() > lines.len();

    let (mut places, mut cancels) = (0u64, 0u64);
    let mut resting: BTreeMap<String, Value> = BTreeMap::new();
    let mut gates: BTreeMap<(String, Option<String>), Gate> = BTreeMap::new();
    let mut acted_markets: HashMap<String, f64> = HashMap::new();
    let mut recent: Vec<Value> = Vec::new();

    for l in &lines {
        let ts = f(l.get("ts")).unwrap_or(0.0);
        if let Some(reasons) = l.get("skip").and_then(Value::as_array) {
            for r in reasons.iter().filter_map(Value::as_str) {
                let g = classify(r);
                let key = (g.gate.clone(), g.scope.clone());
                match gates.get_mut(&key) {
                    // Keep the NEWEST numbers, not the first: the deployed
                    // figure moves as the book does, and a stale one would
                    // describe a headroom that no longer exists.
                    Some(e) => {
                        e.hits += 1;
                        if ts >= e.last_ts {
                            let hits = e.hits;
                            *e = Gate { hits, last_ts: ts, ..g };
                        }
                    }
                    None => {
                        gates.insert(key, Gate { last_ts: ts, ..g });
                    }
                }
            }
            continue;
        }
        if let Some(market) = l.get("place").and_then(Value::as_str) {
            places += 1;
            acted_markets.insert(market.to_string(), ts);
            if let Some(id) = l.get("order_id").and_then(Value::as_str) {
                resting.insert(
                    id.to_string(),
                    json!({ "order_id": id, "market": market, "ts": ts,
                            "venue": l.get("venue"), "side": l.get("side"),
                            "price": l.get("price"), "count": l.get("count") }),
                );
            }
            recent.push(json!({ "kind": "place", "ts": ts, "text": format!(
                "{} {} {} @ {}", market,
                l.get("side").and_then(Value::as_str).unwrap_or(""),
                l.get("count").map(|v| v.to_string()).unwrap_or_default(),
                l.get("price").and_then(Value::as_str).unwrap_or("")) }));
        } else if let Some(market) = l.get("cancel").and_then(Value::as_str) {
            cancels += 1;
            if let Some(id) = l.get("order_id").and_then(Value::as_str) {
                resting.remove(id);
            }
            recent.push(json!({ "kind": "cancel", "ts": ts,
                                "text": format!("{market} ({})",
                                l.get("order_id").and_then(Value::as_str).unwrap_or("")) }));
        } else {
            // Baskets, hedges, take-takes and anything the engine grows later.
            let kind = l
                .get("strategy")
                .or_else(|| l.get("hedge_needed"))
                .and_then(Value::as_str)
                .unwrap_or("event");
            recent.push(json!({ "kind": kind, "ts": ts,
                                "text": serde_json::to_string(l).unwrap_or_default() }));
        }
    }

    let mut constraints: Vec<Gate> = gates.into_values().collect();
    constraints.sort_by_key(|g| std::cmp::Reverse(g.hits));
    let constraint_rows: Vec<Value> = constraints
        .iter()
        .map(|g| {
            let (headroom, free) = break_point(g, class_cap);
            json!({
                "gate": g.gate, "scope": g.scope, "hits": g.hits,
                "deployed": g.deployed, "need": g.need, "limit": g.limit,
                "at": g.at, "headroom": headroom, "free_needed": free,
                "last_age_s": (now - g.last_ts).max(0.0),
                "text": g.text,
                // A gate that names no scope refuses every pair it applies to,
                // so a row must never be read as being about one of them.
                "scoped": g.scope.is_some(),
            })
        })
        .collect();

    // ---- capital, by the same topic buckets the gate uses -----------------
    let marks_path = format!("{}/exec/marks.json", a.data_dir);
    let marks: Value = std::fs::read_to_string(&marks_path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null);
    let mut by_topic: BTreeMap<String, (f64, f64, u64)> = BTreeMap::new();
    for p in marks.get("positions").and_then(Value::as_array).into_iter().flatten() {
        let rel = p.get("relationship_id").and_then(Value::as_str).unwrap_or("");
        let t = topic_of(rel, &topics.topics);
        let e = by_topic.entry(t).or_insert((0.0, 0.0, 0));
        e.0 += f(p.get("cost_usd")).unwrap_or(0.0);
        e.1 += f(p.get("locked_profit_usd")).unwrap_or(0.0);
        e.2 += 1;
    }
    let default_budget = yaml_num(&topics.default_topic_budget);
    let budget_of = |t: &str| -> Option<f64> {
        topics
            .topics
            .iter()
            .find(|x| x.family == t)
            .and_then(|x| x.budget_usd.parse().ok())
            .or(default_budget)
    };
    // Every topic that HOLDS something, plus every topic with a budget — a
    // family sized for capital and holding none of it is a fact about where
    // the book could go, and it is invisible if only holdings are listed.
    let mut names: Vec<String> = by_topic.keys().cloned().collect();
    for t in &topics.topics {
        if !names.contains(&t.family) {
            names.push(t.family.clone());
        }
    }
    names.sort();
    let topic_rows: Vec<Value> = names
        .iter()
        .map(|t| {
            let (cost, locked, n) = by_topic.get(t).copied().unwrap_or((0.0, 0.0, 0));
            let budget = budget_of(t);
            json!({ "topic": t, "deployed_usd": cost, "locked_profit_usd": locked,
                    "positions": n, "budget_usd": budget,
                    "headroom_usd": budget.map(|b| b - cost),
                    "gate": topics.topics.iter().find(|x| x.family == *t)
                              .and_then(|x| x.only_below_util.clone()) })
        })
        .collect();

    // ---- the same capital, derived rather than typed ----------------------
    let derived = derived_capital(a, &marks, &unit, armed, proc.map(|p| p.cmd.as_str()));

    // ---- what is crossing right now --------------------------------------
    let day = crate::integrity::build(&a.data_dir).today;
    let opp_lines = tail_json(&format!("{}/opportunities-{day}.jsonl", a.scan_dir), OPPS_TAIL);
    let mut opps: BTreeMap<String, Opp> = BTreeMap::new();
    let mut opp_first = f64::MAX;
    for o in &opp_lines {
        let Some(rel) = o.get("relationship_id").and_then(Value::as_str) else { continue };
        let ts = f(o.get("ts_local_ns")).map(|n| n / 1e9).unwrap_or(now);
        opp_first = opp_first.min(ts);
        let edge = f(o.get("net_edge_per_contract")).unwrap_or(0.0);
        let total = f(o.get("net_edge_total")).unwrap_or(0.0);
        let e = opps.entry(rel.to_string()).or_default();
        if edge > e.edge {
            e.edge = edge;
            e.total = total;
            e.size = o.get("size").and_then(Value::as_str).unwrap_or("").to_string();
            e.tranche = o.get("tranche").and_then(Value::as_str).unwrap_or("").to_string();
        }
        e.n += 1;
        e.last = e.last.max(ts);
    }
    // market -> relationship, so a place can be credited to the pair it was on;
    // and rel -> tradable, which is the difference between a pair the engine
    // REFUSED and one it never looked at. Conflating those two is the whole
    // failure mode this view exists to avoid: an untradable pair shows a fat
    // edge forever and no gate will ever explain it, because no gate ever ran.
    let reg = arb_registry::Registry::load(&a.registry).ok();
    let allow = arb_registry::Allowlist::load(&a.tradable);
    let mut market_rel: HashMap<String, String> = HashMap::new();
    let mut tradable: HashMap<String, bool> = HashMap::new();
    if let Some(r) = &reg {
        for rel in &r.relationships {
            tradable.insert(rel.id.clone(), rel.tradable(&allow));
            for leg in &rel.legs {
                market_rel.insert(leg.market_id.clone(), rel.id.clone());
            }
        }
    }
    // The armed engine quotes only the ids its `--rel-prefix` selects. A pair
    // outside that scope is not refused either; it is out of frame.
    let scope = proc
        .and_then(|p| p.cmd.split("--rel-prefix ").nth(1))
        .and_then(|r| r.split_whitespace().next())
        .map(str::to_string);
    let acted_rels: HashMap<String, f64> = acted_markets
        .iter()
        .filter_map(|(m, ts)| market_rel.get(m).map(|r| (r.clone(), *ts)))
        .collect();
    let mut opp_rows: Vec<Value> = opps
        .iter()
        .map(|(rel, o)| {
            let topic = topic_of(rel, &topics.topics);
            let is_tradable = tradable.get(rel).copied().unwrap_or(false);
            let in_scope = scope.as_ref().map(|p| rel.starts_with(p)).unwrap_or(true);
            // The refusal that COVERS this pair, from the engine's own stream:
            // a topic-scoped gate on this pair's topic if there is one, else
            // the unscoped gates, which apply to everything. Only asked once
            // the pair is one the engine could have acted on at all.
            let why = if !is_tradable || !in_scope {
                None
            } else {
                constraints
                    .iter()
                    .find(|g| g.scope.as_deref() == Some(topic.as_str()))
                    .or_else(|| constraints.iter().find(|g| g.scope.is_none()))
            };
            // Exactly one of these, and in this order — the first that applies
            // is the whole reason.
            let verdict = if !is_tradable {
                "not tradable"
            } else if !in_scope {
                "out of engine scope"
            } else if acted_rels.contains_key(rel) {
                "acted"
            } else if why.is_some() {
                "refused"
            } else {
                "no refusal seen"
            };
            json!({ "relationship_id": rel, "best_edge": o.edge, "edge_usd": o.total,
                    "size": o.size, "tranche": o.tranche, "observations": o.n,
                    "last_age_s": (now - o.last).max(0.0), "topic": topic,
                    "acted": acted_rels.contains_key(rel),
                    "tradable": is_tradable, "in_scope": in_scope, "verdict": verdict,
                    "blocked_by": why.map(|g| json!({
                        "gate": g.gate, "scope": g.scope, "scoped": g.scope.is_some() })) })
        })
        .collect();
    opp_rows.sort_by(|x, y| {
        f(y.get("best_edge")).partial_cmp(&f(x.get("best_edge"))).unwrap_or(std::cmp::Ordering::Equal)
    });

    // ---- what actually got booked ----------------------------------------
    let booked: Vec<Value> = tail_json(&a.ledger_path, LEDGER_TAIL)
        .into_iter()
        .rev()
        .take(8)
        .map(|r| {
            json!({ "ts": r.get("ts"), "relationship_id": r.get("relationship_id"),
                    "title": r.get("title"), "qty": r.get("qty"),
                    "strategy": r.get("strategy"), "status": r.get("status") })
        })
        .collect();

    recent.reverse();
    recent.truncate(RECENT);

    // A cap the engine is not using is worse than no cap on the screen: every
    // number on this page would be read as the one refusing orders. The engine
    // reads these files ONCE, at `RiskView::load`, so a file touched since it
    // started is a file it has never seen.
    let stale_config: Vec<Value> = [a.exec_config.as_str(), a.topics_config.as_str()]
        .iter()
        .filter_map(|path| {
            let changed = crate::endpoints::age_secs(path)?;
            let up = proc.map(|x| x.up_s)?;
            if changed < up {
                Some(json!({ "path": path, "changed_ago_s": changed, "engine_up_s": up }))
            } else {
                None
            }
        })
        .collect();

    let mut out = Map::new();
    out.insert(
        "engine".into(),
        json!({
            "unit": unit, "running": proc.is_some(), "armed": armed,
            "intents": intents_path,
            "up_s": proc.map(|p| p.up_s), "pid": proc.map(|p| p.pid),
            "summary_age_s": summary_age, "summary_error": summary_err,
            "gauges": gauges, "all": summary,
        }),
    );
    out.insert(
        "window".into(),
        json!({ "seconds": window_s, "asked_for_s": WINDOW_S, "lines": lines.len(),
                "reaches_back": truncated, "places": places, "cancels": cancels,
                "resting": resting.values().cloned().collect::<Vec<_>>() }),
    );
    out.insert("constraints".into(), Value::Array(constraint_rows));
    let marks_age = crate::endpoints::age_secs(&marks_path);
    out.insert(
        "capital".into(),
        json!({ "bankroll_usd": bankroll, "per_class_cap": per_class,
                "class_cap_usd": class_cap,
                "totals": marks.get("totals").cloned().unwrap_or(Value::Null),
                "marks_age_s": marks_age,
                "marks_stale": stale(marks_age, MARKS_STALE_S),
                "topics": topic_rows,
                // Beside the declared figures, never wired to move them:
                // `bankroll_usd` is a policy statement about risk appetite, and
                // a class cap that floated with equity would let this page
                // change what the engine trades.
                "derived": derived }),
    );
    out.insert(
        "opportunities".into(),
        json!({ "window_s": if opp_first < f64::MAX { now - opp_first } else { 0.0 },
                "scope": scope, "rows": opp_rows }),
    );
    out.insert("stale_config".into(), Value::Array(stale_config));
    out.insert("recent".into(), Value::Array(recent));
    out.insert("booked".into(), Value::Array(booked));
    out.insert("generated_at".into(), json!(now));
    serde_json::to_string(&Value::Object(out)).unwrap_or_else(|_| "{}".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four gates that actually fire on this book, parsed from the exact
    /// strings `arb_core::risk::gate` builds. These literals are the contract:
    /// if the engine's wording changes, this fails rather than the panel
    /// quietly showing an unparsed row.
    #[test]
    fn the_engines_own_numbers_are_read_back_out_of_its_refusals() {
        let g = classify("class cap: 331.7+2.5 > 343.00");
        assert_eq!(g.gate, "class cap");
        assert_eq!((g.deployed, g.need, g.limit), (Some(331.7), Some(2.5), Some(343.0)));
        assert_eq!(g.scope, None, "the class cap names no scope and covers everything");

        let g = classify("topic budget [time-poty-26]: 154+5 > 150");
        assert_eq!(g.gate, "topic budget");
        assert_eq!(g.scope.as_deref(), Some("time-poty-26"));
        assert_eq!((g.deployed, g.need, g.limit), (Some(154.0), Some(5.0), Some(150.0)));

        let g = classify("topic [other] gated to util<0.5: at 0.86");
        assert_eq!(g.gate, "low-utilisation gate");
        assert_eq!(g.scope.as_deref(), Some("other"));
        assert_eq!((g.limit, g.at), (Some(0.5), Some(0.86)));

        let g = classify("insufficient kalshi balance: 12.50 > 4.00");
        assert_eq!(g.gate, "venue cash");
        assert_eq!(g.scope.as_deref(), Some("kalshi"));
        assert_eq!((g.need, g.limit), (Some(12.5), Some(4.0)));
    }

    /// The gate appends `(overflow needs apr>=N)` to the same reason. The
    /// numbers in front of it are still the numbers.
    #[test]
    fn an_overflow_note_does_not_break_the_numbers() {
        let g = classify("class cap: 331.7+2.5 > 343.00 (overflow needs apr>=25)");
        assert_eq!((g.deployed, g.need, g.limit), (Some(331.7), Some(2.5), Some(343.0)));
    }

    /// A reason this parser has never seen must still COUNT and still show its
    /// text. Dropping it would report "nothing is blocking us" while the
    /// engine refuses every order.
    #[test]
    fn an_unrecognised_reason_survives_as_itself() {
        let g = classify("kill switch: data/KILL present");
        assert_eq!(g.gate, "kill switch");
        assert_eq!(g.text, "kill switch: data/KILL present");
        assert_eq!(g.deployed, None, "and invents no numbers for it");
    }

    /// The break point: what has to come free before the refused order fits.
    #[test]
    fn the_break_point_is_a_subtraction_of_the_engines_own_numbers() {
        let g = classify("class cap: 331.7+2.5 > 343.00");
        let (headroom, free) = break_point(&g, Some(343.0));
        assert!((headroom.unwrap() - 11.3).abs() < 1e-9);
        assert_eq!(free, None, "2.5 fits inside 11.3 — this refusal was a bigger order");

        let g = classify("class cap: 341+5 > 343.00");
        let (headroom, free) = break_point(&g, Some(343.0));
        assert!((headroom.unwrap() - 2.0).abs() < 1e-9);
        assert!((free.unwrap() - 3.0).abs() < 1e-9, "$3 must come off before $5 fits");
    }

    /// A topic past its budget reports how far past, not zero. Clamping it
    /// would read as "exactly full", which is a different and much less
    /// alarming fact than "$4 over".
    #[test]
    fn a_topic_over_its_budget_reads_as_over_not_full() {
        let g = classify("topic budget [time-poty-26]: 154+5 > 150");
        let (headroom, free) = break_point(&g, Some(343.0));
        assert!((headroom.unwrap() + 4.0).abs() < 1e-9, "-4, not 0");
        assert!((free.unwrap() - 9.0).abs() < 1e-9, "$4 over plus the $5 asked for");
    }

    /// The utilisation gate is a fraction, and an operator cannot act on a
    /// fraction. It is expressed in the dollars that must come off the book,
    /// which needs the class cap — and without one it stays absent rather
    /// than being guessed.
    #[test]
    fn the_utilisation_gate_is_reported_in_dollars_or_not_at_all() {
        let g = classify("topic [other] gated to util<0.5: at 0.86");
        let (_, free) = break_point(&g, Some(343.0));
        assert!((free.unwrap() - 123.48).abs() < 1e-6, "(0.86-0.5) * 343");
        assert_eq!(break_point(&g, None).1, None, "no cap, no dollars");
    }

    /// A `+` inside a topic name must not be read as the deployed/need split,
    /// and a reason that is only partly numeric must not yield a partly
    /// invented row.
    #[test]
    fn a_malformed_reason_yields_no_numbers_rather_than_wrong_ones() {
        let g = classify("topic budget [odd]: not-a-number+5 > 150");
        assert_eq!(g.deployed, None);
        assert_eq!(g.limit, Some(150.0), "what IS readable is still read");
        assert_eq!(break_point(&g, None).0, None, "and no headroom is fabricated");
    }

    /// The trap a cap change sets: `exec.yaml` is read once at startup, so
    /// editing it changes what this dashboard reads and NOT what the engine
    /// enforces, until a restart. The comparison that catches it is the file's
    /// age against the engine's uptime — a file younger than the process is
    /// one the process never read.
    #[test]
    fn a_config_touched_after_the_engine_started_is_one_it_has_never_read() {
        let stale = |changed: u64, up: u64| changed < up;
        assert!(stale(60, 3600), "edited a minute ago, engine up an hour");
        assert!(!stale(7200, 3600), "edited before the engine started: it read this");
        assert!(!stale(3600, 3600), "same instant is not evidence of a later edit");
    }

    // ----- capital, derived -------------------------------------------------

    /// A data dir with the files this panel reads, so the emission site is
    /// exercised over REAL files. `Args::for_test` points every path at
    /// `/nonexistent` on purpose — a test that silently picked up this machine's
    /// `data/` would pass here and nowhere else — and that is exactly what left
    /// the money columns below unpinned: nothing could reach them. Per test and
    /// per pid, because `cargo test` runs these in parallel in one process.
    fn scratch(tag: &str) -> String {
        let d = std::env::temp_dir().join(format!("arb-dash-cap-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(d.join("exec")).expect("a scratch dir");
        d.to_string_lossy().into_owned()
    }

    fn measured(usd: Option<f64>, age_s: Option<u64>, stale_past_s: Option<u64>) -> Measured {
        Measured { usd, from: "f".into(), age_s, stale_past_s, stale_means: "m".into() }
    }

    /// The recoverable figure is Σ `liq_value_usd` and never the shortcut
    /// `totals.cost_usd + totals.mark_pnl_usd`. `marks::build` skips a row it
    /// could not price when it sums the mark but adds EVERY row's cost, so the
    /// shortcut carries an unpriceable row at full cost — reporting capital as
    /// recoverable exactly where the book is least knowable. The shortcut is
    /// right on today's file only because all 58 rows priced.
    #[test]
    fn an_unpriceable_row_is_counted_as_unknown_not_as_recoverable_at_cost() {
        let marks = json!({
            "positions": [
                { "cost_usd": 100.0, "liq_value_usd": 90.0, "mark_pnl_usd": -10.0 },
                { "cost_usd": 50.0,  "liq_value_usd": null, "mark_pnl_usd": null },
            ],
            "totals": { "cost_usd": 150.0, "mark_pnl_usd": -10.0, "n_open": 2 }
        });
        let d = deployed(&marks);
        assert_eq!(
            d.share_value_usd,
            Some(90.0),
            "the row that could not be priced contributes nothing"
        );
        assert_eq!(d.unpriced_rows, 1, "and is COUNTED, so the 90 is read as partial");
        assert_eq!(d.unmarked_records, Some(0), "both open records did produce a row");
        assert!(
            150.0 - 10.0 > d.share_value_usd.expect("a value"),
            "the shortcut would have claimed 140"
        );
    }

    /// THE GAP IS NOT ALL RIDERS. `n_open - positions.len()` is every open
    /// record that produced no row, and `marks::build` produces no row in TWO
    /// cases: a single-leg directional rider, which has no basket to mark, and a
    /// two-leg basket it could not price, which it also counts in
    /// `unpriced_positions`. Its own identity says so —
    /// `n_open - positions.len() - unpriced_positions == single-leg records` —
    /// and the first version of this panel published the left-hand side while
    /// calling it the right-hand one. An unmarkable basket is money sitting at a
    /// venue; attributing it to a rider is how it stops being looked for.
    #[test]
    fn an_unmarkable_basket_is_not_reported_as_a_directional_rider() {
        let marks = json!({
            "positions": [{ "liq_value_usd": 42.0 }],
            "totals": { "n_open": 3, "unpriced_positions": 1 }
        });
        let d = deployed(&marks);
        assert_eq!(d.share_value_usd, Some(42.0));
        assert_eq!(d.unmarked_records, Some(2), "three open records, one row");
        assert_eq!(d.unpriced_baskets, Some(1), "one two-leg basket that could not be priced");
        assert_eq!(d.single_leg_records, Some(1), "and only ONE rider, not two");
    }

    /// The live file today: 59 open, 58 rows, 0 unpriced. Here the counter
    /// really does read 0 while a record was in fact dropped, which is why the
    /// subtraction is published at all — but that is a fact about this file, not
    /// a rule, and the panel must not state it as one.
    #[test]
    fn a_record_that_was_never_marked_is_reported_even_when_nothing_was_unpriced() {
        let marks = json!({
            "positions": [{ "liq_value_usd": 42.0 }],
            "totals": { "n_open": 2, "unpriced_positions": 0 }
        });
        let d = deployed(&marks);
        assert_eq!(d.unmarked_records, Some(1), "one open record produced no row at all");
        assert_eq!(d.single_leg_records, Some(1), "and this time it IS the rider");
    }

    /// No marks file is UNKNOWN, not zero. A $0.00 under "recoverable" reads as
    /// "the book is flat", which is the opposite of what an absent file means:
    /// the engine that writes it is not running.
    #[test]
    fn an_absent_marks_file_reports_unknown_rather_than_a_flat_book() {
        let d = deployed(&Value::Null);
        assert_eq!(d.share_value_usd, None);
        assert_eq!(d.unmarked_records, None);
        assert_eq!(d.unpriced_baskets, None);
        assert_eq!(d.single_leg_records, None);
    }

    /// THE DOUBLE-COUNT TRAP, pinned against the live file's own numbers.
    /// PM-US reports `currentBalance = buyingPower + marginRequirement`, and
    /// the $1.00 per short contract behind `marginRequirement` is already
    /// inside each position's cost basis — so it is already inside the deployed
    /// figure this sits beside. Taking `currentBalance` here counts $323.86 of
    /// collateral twice, which is the bug the 2026-08-14 accounting audit found.
    #[test]
    fn the_pmus_cash_figure_is_buying_power_and_not_the_margin_inflated_balance() {
        let live = r#"{"currentBalance": 653.15805, "currency": "USD", "buyingPower": 329.29805,
                       "openOrders": 0, "unsettledFunds": 0, "marginRequirement": 323.86}"#;
        assert_eq!(spendable_pmus(live), Some(329.29805), "not 653.15805, and not 653.15805−0");
        assert_eq!(spendable_pmus("not json"), None, "and an unreadable snapshot is unknown");
    }

    /// The constants the ARMED risk gate is spending against, off its command
    /// line. The tracked unit file says `kalshi=340.09`; the drop-in that arms
    /// the engine overrides it with 320.43, so a figure read out of this repo
    /// is a figure nobody is trading against.
    #[test]
    fn the_balances_the_engine_is_spending_against_come_off_its_command_line() {
        let cmd = "arb-trader --rel-prefix xvus- --balance kalshi=320.43 \
                   --balance polymarket_us=301.92 --enable-orders --yes-trade-live";
        assert_eq!(
            engine_balances(cmd),
            vec![("kalshi".into(), 320.43), ("polymarket_us".into(), 301.92)]
        );
    }

    /// A venue the engine was never given a balance for must be ABSENT, not
    /// zero. `RiskView` fails closed on a missing balance, so a $0.00 row would
    /// read as "that venue is out of cash" rather than "it was never funded".
    #[test]
    fn a_venue_with_no_balance_flag_yields_no_row_rather_than_a_zero() {
        assert!(engine_balances("arb-trader --socket data/arbbot.sock").is_empty());
        assert!(engine_balances("--balance kalshi=").is_empty(), "and an empty value is not 0");
    }

    /// `armed_engine` falls back to ANY `arb-trader`, and this host runs an
    /// unarmed shadow whose `--balance kalshi=340.09` is $0.0004 from this
    /// dashboard's own `--kalshi-balance 340.0896`. So with the armed slice
    /// stopped, a column filled from that fallback shows the shadow's constants
    /// under an armed heading and the one gap here that can fail collapses to
    /// ~$0: the broken state renders as an all-clear. The column is filled off
    /// an armed process or not at all, and it always names the unit.
    #[test]
    fn the_engine_column_is_filled_off_an_armed_process_or_not_at_all() {
        let shadow = "arb-trader --balance kalshi=340.09 --balance polymarket_us=349.42 \
                      --out data/trader-rs/intents.jsonl";
        let (bal, from) = engine_cash("arbbot-trader-rs", false, Some(shadow));
        assert!(bal.is_empty(), "340.09 against 340.0896 would have read as agreement");
        assert!(from.contains("arbbot-trader-rs"), "{from}");
        assert!(from.contains("NOT armed"), "{from}");

        let armed = "arb-trader --balance kalshi=320.43 --enable-orders --yes-trade-live";
        let (bal, from) = engine_cash("arbbot-trader-m3", true, Some(armed));
        assert_eq!(bal, vec![("kalshi".into(), 320.43)]);
        assert!(from.contains("arbbot-trader-m3"), "{from}");
        assert!(from.contains("ARMED"), "{from}");

        let (bal, from) = engine_cash("", false, None);
        assert!(bal.is_empty());
        assert_eq!(from, "no arb-trader is running");
    }

    /// The two hand-typed constants disagree TODAY — the armed engine seeds its
    /// gate with `kalshi=320.43` while this dashboard was started with
    /// `--kalshi-balance 340.0896` — and no page in this repo said so. Either
    /// can be edited alone in its own unit file, and nothing moves either of
    /// them, which is what leaves this the one cash comparison on the panel that
    /// can genuinely fail.
    ///
    /// AND `venue_diff_usd` IS NOT A KEY ANY MORE. `is_null()` would pass on a
    /// deleted key and on a nulled one alike, so this asks the object, which is
    /// the only assertion that fails if the subtraction comes back.
    #[test]
    fn the_engines_balance_and_the_dashboards_are_shown_with_the_gap_between_them() {
        let m = measured(None, None, None);
        let r = cash_row("kalshi", Some(320.43), Some(340.0896), &m);
        assert!((r["dash_diff_usd"].as_f64().expect("a gap") + 19.6596).abs() < 1e-9);
        assert!(r["venue_usd"].is_null(), "and neither constant is dressed up as a measurement");
        assert!(
            r.as_object().expect("a row").get("venue_diff_usd").is_none(),
            "a constant minus a measurement is deployed capital, not a discrepancy"
        );
        assert!(r["venue_stale"].is_null(), "an unknown age is not a fresh one");
    }

    /// A measured figure carries its age, the BOUND IT WAS JUDGED AGAINST, and
    /// what going stale costs — and the last of those is per-source, which is
    /// the point. The bound rides in from the source rather than from a constant
    /// in this file, so a row whose source publishes none says UNKNOWN rather
    /// than borrowing somebody else's window.
    #[test]
    fn a_measured_figure_is_judged_against_its_own_published_bound() {
        let hour = crate::endpoints::books::SNAPSHOT_STALE_S;
        let row = |age, bound| {
            cash_row("polymarket_us", Some(301.92), None, &measured(Some(329.29805), age, bound))
        };
        let fresh = row(Some(30), Some(hour));
        assert_eq!(fresh["venue_stale"], false);
        assert_eq!(fresh["venue_stale_past_s"], hour, "and the row says what it was judged on");
        let old = row(Some(1_963_718), Some(hour));
        assert_eq!(old["venue_stale"], true, "the live snapshot's own age");
        assert_eq!(old["venue_usd"], 329.29805, "the number still shows — that much is true");
        assert_eq!(old["venue_stale_means"], "m", "with what its age costs, from the source");
        let no_bound = row(Some(1_963_718), None);
        assert!(no_bound["venue_stale"].is_null(), "no published bound, no verdict");
        assert_eq!(no_bound["venue_age_s"], 1_963_718, "the age is still reported honestly");
    }

    /// Stale is a THIRD state, not a pass, and an age nobody could establish is
    /// unknown rather than fresh. The engine's marks go through here against
    /// their own 120s heartbeat; the two cash sources bring their own bounds.
    #[test]
    fn a_figure_with_no_age_is_unknown_and_one_past_its_window_says_so() {
        assert_eq!(stale(None, MARKS_STALE_S), None);
        assert_eq!(stale(Some(MARKS_STALE_S), MARKS_STALE_S), Some(false), "the line is exclusive");
        assert_eq!(stale(Some(MARKS_STALE_S + 1), MARKS_STALE_S), Some(true));
    }

    /// THE CONTRACT WITH THE ENGINE'S BALANCE POLL, as `publish_balances`
    /// actually writes it: venue figures under `balances` as STRINGS, the read
    /// time under `at`, and the gate's own expiry under `max_age_s`. The first
    /// version of this panel read `venue_cash.json` and a `ts` key — a file
    /// nothing writes and a name nothing publishes — so the measured column
    /// could never populate at all while six sentences said it did.
    ///
    /// `max_age_s` is read rather than re-transcribed: it is the bound the GATE
    /// is using, and copying 180 into this file is how the two drift apart.
    #[test]
    fn the_balance_snapshot_is_read_at_the_contract_the_engine_actually_publishes() {
        let (cash, at, bound) = balances_snapshot(
            r#"{"at":1755600000,"max_age_s":180,
                "balances":{"kalshi":"301.11","polymarket_us":"288.4"}}"#,
        );
        assert_eq!(cash.get("kalshi"), Some(&301.11), "the figures are venue STRINGS");
        assert_eq!(cash.len(), 2, "and neither `at` nor `max_age_s` became a third venue");
        assert_eq!(at, Some(1755600000.0));
        assert_eq!(bound, Some(180), "the gate's own bound, off the file");

        let (_, _, none) = balances_snapshot(r#"{"at":1,"balances":{"kalshi":"5"}}"#);
        assert_eq!(none, None, "a file with no bound has an unknown staleness, not a default");
        assert!(
            balances_snapshot(r#"{"ts":1,"kalshi":301.11}"#).0.is_empty(),
            "the pre-#77 shape this panel invented is not written by anything"
        );
        assert!(balances_snapshot(r#"{"balances":{"kalshi":"n/a"}}"#).0.is_empty(), "not money");
        assert!(balances_snapshot("not json").0.is_empty(), "an unreadable file measures nothing");
    }

    /// WHAT THE GATE IS ACTUALLY SPENDING AGAINST, which is the fact that stops
    /// the measured column reading as a second opinion. Inside its published
    /// bound the gate spends against that very reading; past it the gate holds
    /// no cash at all and refuses everything — opposite states behind the same
    /// displayed number. An absent file cannot separate "has not answered yet"
    /// from "never will", and says so rather than picking one.
    #[test]
    fn the_panel_says_which_figure_the_gate_is_spending_against() {
        let p = "data/exec/balances.json";
        assert_eq!(gate_state(false, p, Some(1), Some(180)).0, "none", "no armed engine, no gate");
        assert_eq!(gate_state(true, p, Some(180), Some(180)).0, "live", "the bound is inclusive");
        let (on, why) = gate_state(true, p, Some(181), Some(180));
        assert_eq!(on, "stale");
        assert!(why.contains("REFUSING EVERY ORDER"), "{why}");
        assert_eq!(gate_state(true, p, None, Some(180)).0, "unknown", "no file: seed or refusing");
        assert_eq!(gate_state(true, p, Some(1), None).0, "unknown", "no bound, no verdict");
    }

    /// THE MONEY COLUMNS, at the site that emits them, over a file written in
    /// the shape `arb-trader`'s `publish_balances` really writes: nulling any of
    /// them, serving one under another's key, or going back to the invented
    /// `venue_cash.json`/`ts` contract fails here.
    ///
    /// THIS IS THE BLOCKER THE FIRST VERSION SHIPPED. The measured column read a
    /// path nothing writes and a timestamp key nothing publishes, so it could
    /// never populate on any machine — and every test that touched it wrote the
    /// invented file itself and passed.
    #[test]
    fn the_money_columns_are_pinned_at_the_site_that_emits_them() {
        let dir = scratch("emit");
        std::fs::write(
            format!("{dir}/exec/balances.json"),
            format!(
                r#"{{"at": {}, "max_age_s": 180,
                     "balances": {{"kalshi": "301.11", "polymarket_us": "288.4"}}}}"#,
                now_secs() - 30
            ),
        )
        .expect("a balance snapshot");
        let mut a = Args::for_test();
        a.data_dir = dir;
        a.kalshi_balance = Some("340.0896".into());
        let marks = json!({
            "positions": [{ "liq_value_usd": 90.0 }, { "liq_value_usd": 42.5 }],
            "totals": { "n_open": 2, "unpriced_positions": 0 }
        });
        let cmd = "arb-trader --balance kalshi=320.43 --balance polymarket_us=301.92 \
                   --enable-orders --yes-trade-live";
        let v = derived_capital(&a, &marks, "arbbot-trader-m3", true, Some(cmd));

        assert_eq!(v["share_value_usd"], 132.5, "Σ liq_value_usd, at the key it is served under");
        assert_eq!(v["unpriced_rows"], 0);
        assert_eq!(v["unmarked_records"], 0);
        assert!(v["engine_from"].as_str().expect("provenance").contains("arbbot-trader-m3"));
        assert_eq!(v["gate_on"], "live", "a reading inside the bound IS what the gate spends");

        let rows = v["cash"].as_array().expect("one row per venue");
        let k = &rows[0];
        assert_eq!(k["venue"], "kalshi");
        assert_eq!(k["engine_usd"], 320.43, "the --balance seed, off /proc");
        assert_eq!(k["dash_usd"], 340.0896, "what this process was started with");
        assert!((k["dash_diff_usd"].as_f64().expect("the constants' gap") + 19.6596).abs() < 1e-9);
        assert_eq!(k["venue_usd"], 301.11, "and what the engine last read off the venue");
        assert_eq!(k["venue_stale"], false);
        assert_eq!(k["venue_stale_past_s"], 180, "judged on the bound the engine published");
        assert_eq!(rows[1]["venue_usd"], 288.4, "the other venue is measured too");
    }

    /// With the armed slice stopped, `armed_engine` hands this panel the SHADOW.
    /// Nothing of the shadow's may appear in a money column: its `kalshi=340.09`
    /// against `--kalshi-balance 340.0896` is a $0.0004 gap, which renders as
    /// agreement, on a panel whose entire job is to show that gap.
    #[test]
    fn a_shadows_constants_never_fill_a_column_the_page_heads_as_armed() {
        let dir = scratch("shadow");
        let mut a = Args::for_test();
        a.data_dir = dir;
        a.kalshi_balance = Some("340.0896".into());
        let shadow = "arb-trader --balance kalshi=340.09 --balance polymarket_us=349.42 \
                      --out data/trader-rs/intents.jsonl";
        let v = derived_capital(&a, &Value::Null, "arbbot-trader-rs", false, Some(shadow));
        let k = &v["cash"].as_array().expect("rows")[0];
        assert_eq!(k["venue"], "kalshi");
        assert!(k["engine_usd"].is_null(), "no risk gate is spending against this");
        assert!(k["dash_diff_usd"].is_null(), "so there is no gap, not a $0.0004 one");
        assert!(v["engine_from"].as_str().expect("provenance").contains("NOT armed"));
        assert_eq!(v["gate_on"], "none", "and nothing here is a figure a gate is spending");
    }

    /// A reading past the bound the engine published for it is served as stale
    /// and aged off its OWN `at`, never off the file's mtime — the age is most
    /// of why this file is read at all, and a file with no `at` is not this
    /// writer's file. Here the sentence beside the age is the true one: the gate
    /// really has stopped, because `RiskView::spendable` hands back no pairs
    /// past that bound and every venue then looks up $0.
    #[test]
    fn an_expired_engine_reading_is_stale_and_says_the_gate_has_stopped() {
        let dir = scratch("expired");
        std::fs::write(
            format!("{dir}/exec/balances.json"),
            format!(r#"{{"at": {}, "max_age_s": 180, "balances": {{"kalshi": "301.11"}}}}"#,
                    now_secs() - 4 * 3600),
        )
        .expect("a balance snapshot");
        let mut a = Args::for_test();
        a.data_dir = dir;
        let v = derived_capital(&a, &Value::Null, "u", true, Some("--balance kalshi=320.43"));
        assert_eq!(v["gate_on"], "stale");
        let k = &v["cash"].as_array().expect("rows")[0];
        assert_eq!(k["venue_usd"], 301.11, "the number still shows — that much is true");
        assert_eq!(k["venue_stale"], true);
        assert!(k["venue_age_s"].as_u64().expect("an age") >= 4 * 3600, "not the file's mtime");
        let means = k["venue_stale_means"].as_str().expect("what the age costs");
        assert!(means.contains("stopped spending against it"), "{means}");
        assert!(k["venue_from"].as_str().expect("provenance").contains("balance poll"));
    }

    /// THE PM-US FALLBACK, which until #77 was the ONLY measured figure this
    /// panel ever served in production and which no test reached: it could be
    /// deleted, mis-keyed onto kalshi, or judged against the wrong window with
    /// the suite green. It is buyingPower and never currentBalance, it lands on
    /// the PM-US row alone, it is judged against the books page's hour because
    /// it is the books page's file, and — M1 — the sentence beside its age is
    /// NOT the one above. No risk gate has ever read this file, so its ageing
    /// stopped nothing; borrowing the engine's alarm for it would raise a fault
    /// against a snapshot the engine never consulted.
    #[test]
    fn the_hand_fetched_pmus_snapshot_is_the_fallback_and_says_so_in_its_own_words() {
        let dir = scratch("pmus");
        std::fs::write(
            format!("{dir}/pmus_balances.json"),
            r#"{"currentBalance": 653.15805, "buyingPower": 329.29805,
                "marginRequirement": 323.86}"#,
        )
        .expect("a hand-fetched pm-us snapshot");
        let mut a = Args::for_test();
        a.data_dir = dir.clone();
        a.pmus_dir = dir;
        a.kalshi_balance = Some("340.0896".into());
        let v = derived_capital(&a, &Value::Null, "u", true, Some("--balance kalshi=320.43"));
        assert_eq!(v["gate_on"], "unknown", "no snapshot: the seed may or may not still be live");
        let rows = v["cash"].as_array().expect("rows");
        assert!(rows[0]["venue_usd"].is_null(), "kalshi is NOT measured by a pm-us file");
        let p = rows.iter().find(|r| r["venue"] == "polymarket_us").expect("a pm-us row");
        assert_eq!(p["venue_usd"], 329.29805, "buyingPower, never the 653.15805 currentBalance");
        assert_eq!(
            p["venue_stale_past_s"],
            crate::endpoints::books::SNAPSHOT_STALE_S,
            "the books page's hour, because this is the books page's file"
        );
        let means = p["venue_stale_means"].as_str().expect("what its age costs");
        assert!(means.contains("no risk gate has ever read this file"), "{means}");
        assert!(!means.contains("stopped spending"), "the gate never spent against this: {means}");
        assert!(p["venue_from"].as_str().expect("provenance").contains("fetched by hand"));
    }

    /// And with no snapshot at all — the state this repo was in until #77 — the
    /// measured side says it is unavailable and NAMES the file that would fill
    /// it, rather than showing a three-week-old number or a $0. `gate_on` is
    /// honest about the same hole: an absent file cannot tell a poll that has
    /// not answered yet from one that never will.
    #[test]
    fn with_no_snapshot_the_measured_side_says_unavailable_rather_than_guessing() {
        let dir = scratch("nosnap");
        let mut a = Args::for_test();
        a.data_dir = dir.clone();
        a.kalshi_balance = Some("340.0896".into());
        let v = derived_capital(&a, &Value::Null, "u", true, Some("--balance kalshi=320.43"));
        let k = &v["cash"].as_array().expect("rows")[0];
        assert!(k["venue_usd"].is_null());
        assert!(k["venue_stale"].is_null(), "unknown is not fresh");
        let from = k["venue_from"].as_str().expect("a reason");
        assert!(from.contains(&format!("{dir}/exec/balances.json")), "{from}");
        assert!(from.contains("does not exist"), "{from}");
        assert_eq!(v["gate_on"], "unknown");
        let why = v["gate_why"].as_str().expect("a reason");
        assert!(why.contains("balances_source"), "and it names what would settle it: {why}");
        assert!(v["share_value_usd"].is_null(), "and no marks file is unknown, not a flat book");
    }

    /// The payload itself, over real files. The `derived` block is what this
    /// whole panel was added for and nothing reached it: `Args::for_test` points
    /// every path at `/nonexistent`, so `json()` could serve any number here, or
    /// none, and the suite stayed green.
    #[test]
    fn the_capital_payload_carries_the_derived_figures_over_real_files() {
        let dir = scratch("payload");
        std::fs::write(
            format!("{dir}/exec/marks.json"),
            json!({
                "positions": [{ "liq_value_usd": 90.0 }, { "liq_value_usd": null }],
                "totals": { "cost_usd": 150.0, "n_open": 4, "unpriced_positions": 1 }
            })
            .to_string(),
        )
        .expect("a marks file");
        let mut a = Args::for_test();
        a.data_dir = dir;
        let v: Value = serde_json::from_str(&json(&a)).expect("a payload");
        let c = &v["capital"]["derived"];
        assert_eq!(c["share_value_usd"], 90.0, "Σ liq_value_usd, not totals.cost_usd");
        assert_eq!(c["unpriced_rows"], 1, "the row with no bid to liquidate into");
        assert_eq!(c["unmarked_records"], 2, "four open records, two rows");
        assert_eq!(c["unpriced_baskets"], 1, "one of the two is a basket that could not price");
        assert_eq!(c["single_leg_records"], 1, "and only the OTHER one is a rider");
        assert_eq!(v["capital"]["marks_stale"], false, "written a moment ago");
    }

    /// `topic_of` is arb-core's, so a position is bucketed exactly the way the
    /// gate that refused it was bucketed. Longest match wins.
    #[test]
    fn positions_are_bucketed_by_the_gates_own_function() {
        let topics: Vec<TopicIn> = serde_yaml::from_str(
            "- {family: time-poty-26, budget_usd: '150'}\n- {family: nobel-peace-26, budget_usd: '80'}",
        )
        .expect("topics");
        assert_eq!(topic_of("xvus-time-poty-26-zohranmamdani", &topics), "time-poty-26");
        assert_eq!(topic_of("xvus-btcmax-26-rung3", &topics), "other");
    }
}
