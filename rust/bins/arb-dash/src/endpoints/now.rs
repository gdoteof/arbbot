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

use std::collections::{BTreeMap, HashMap};

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
const HEADLINE: [(&str, &str); 12] = [
    ("risk_allowed", "orders the risk gate passed"),
    ("risk_rejected", "orders the risk gate refused"),
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
    let marks: Value = std::fs::read_to_string(format!("{}/exec/marks.json", a.data_dir))
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
    //
    // A CORRECTION IS METADATA ABOUT A BASKET, NOT A BASKET. It carries a
    // `relationship_id` and a `ts` and nothing else this panel reads — no qty,
    // no strategy, no title — so listing one renders a blank row, and because
    // it is written LATER than the record it corrects it sorts to the top and
    // pushes real fills off the end.
    //
    // `ledger::apply_corrections` has always dropped them; this panel read the
    // raw tail and did not. Seen 2026-08-25: ten corrections appended in one
    // pass filled all eight slots, and the `/now` tab showed no fills at all on
    // the night the exit path first started closing.
    //
    // Filtered rather than APPLIED, deliberately: applying needs the whole file
    // (a correction may name a record older than the tail), and this endpoint
    // is a tail by construction. `/api/trades` is the one that folds them in,
    // and it already does.
    let booked: Vec<Value> = tail_json(&a.ledger_path, LEDGER_TAIL)
        .into_iter()
        .filter(is_booked_basket)
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
    out.insert(
        "capital".into(),
        json!({ "bankroll_usd": bankroll, "per_class_cap": per_class,
                "class_cap_usd": class_cap,
                "totals": marks.get("totals").cloned().unwrap_or(Value::Null),
                "marks_age_s": crate::endpoints::age_secs(
                    &format!("{}/exec/marks.json", a.data_dir)),
                "topics": topic_rows }),
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

/// Is this ledger record a basket the engine booked, or bookkeeping about one?
///
/// See the `booked` panel for why this exists. Pulled out as a named predicate
/// so it can be tested: the panel itself needs an `App` and a ledger on disk.
fn is_booked_basket(r: &Value) -> bool {
    r.get("status").and_then(Value::as_str) != Some("correction")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A CORRECTION IS NOT A FILL, and the `/now` tab showed nothing else for
    /// the eight slots it has on 2026-08-25 — ten corrections appended in one
    /// pass, each newer than the record it corrects, on the night the exit path
    /// first started closing baskets.
    #[test]
    fn a_correction_is_not_something_the_engine_booked() {
        let corr = serde_json::json!({
            "ts": 1787632310.3, "status": "correction",
            "relationship_id": "xvus-time-poty-26-popeleoxiv",
            "corrects_ts": 1787631074.9, "fields": {"legs": []}
        });
        assert!(!is_booked_basket(&corr), "it carries a rel_id and nothing else this panel reads");

        for status in ["open", "unwound", "settled"] {
            let rec = serde_json::json!({
                "ts": 1787631074.9, "status": status, "qty": 5,
                "relationship_id": "xvus-time-poty-26-popeleoxiv", "strategy": "maker-exit"
            });
            assert!(is_booked_basket(&rec), "{status} is a basket");
        }
        // A record with no status at all is still a basket: the panel has
        // always shown those, and silence here would hide them.
        assert!(is_booked_basket(&serde_json::json!({"ts": 1.0, "qty": 5})));
    }

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
