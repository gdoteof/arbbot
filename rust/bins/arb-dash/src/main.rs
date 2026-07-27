//! arb-dash — instrument over the double-entry books.
//!
//! Binds 127.0.0.1 only and never touches a venue order path: it holds no
//! credentials and cannot place, cancel or move an order.
//!
//! It is read-only with ONE exception: `POST /api/rollup` rebuilds the local
//! ToB series. That writes files under the rollup dir and burns ~30s of two
//! cores, so it is deliberately POST-only (a prefetch or crawler cannot fire
//! it), single-flight (one build at a time), and atomic (temp file + rename,
//! so a reader never sees a half-built series). Deliberately runs on a DIFFERENT port from the Python
//! dash (4748) so both can be open side by side while the numbers are compared.
//!
//! No HTTP crate: the workspace's only dependencies are serde/serde_json, and a
//! localhost read-only instrument does not justify pulling in a runtime.
//!
//!   arb-dash --kalshi-dir <dir> --pmus-dir <dir> --pmus-deposits <usd> \
//!            [--data-dir data] [--port 4749] [--kalshi-balance <usd>]

mod integrity;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::process::exit;

use arb_core::fees::FeeSchedule;
use arb_core::model::Venue;
use arb_core::scan::Cx;
use arb_ledger::kalshi::{Deposit, Fill, KalshiImport, Settlement};
use arb_ledger::pmus::{Balances, PmusImport, Position};
use arb_ledger::{accounts, report, Journal};
use arb_query::{intents, opps, sources_for_range};
use arb_registry::{Allowlist, Registry};
use arb_scenario::{price, Quote, Scenario};
use arb_tob::{build_day, series, DEFAULT_INTERVAL_NS};

const PAGE: &str = include_str!("index.html");

struct Args {
    kalshi_dir: String,
    pmus_dir: String,
    pmus_deposits: String,
    kalshi_balance: Option<String>,
    data_dir: String,
    scan_dir: String,
    raw_dir: String,
    parquet_dir: String,
    rollup_dir: String,
    intents_path: String,
    registry: String,
    tradable: String,
    port: u16,
}

/// The one piece of mutable state in the process, and it exists only so a
/// second trigger cannot start while a build is in flight.
#[derive(Default)]
struct Rollup {
    running_day: Option<String>,
    last: Option<serde_json::Value>,
}

type Shared = Arc<Mutex<Rollup>>;

fn rollup_status(sh: &Shared) -> String {
    let g = sh.lock().unwrap_or_else(|e| e.into_inner());
    serde_json::json!({
        "running": g.running_day,
        "last": g.last,
    })
    .to_string()
}

/// Start a build if one is not already running. Returns immediately — the
/// build runs on its own thread so the single-threaded accept loop keeps
/// serving while ~30s of work happens.
fn rollup_start(a: &Args, sh: &Shared, query: &str) -> String {
    let day = query_param(query, "day")
        .unwrap_or_else(|| integrity::build(&a.data_dir).today);
    {
        let mut g = sh.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(d) = &g.running_day {
            return serde_json::json!({
                "started": false,
                "reason": format!("a build for {d} is already running"),
                "running": d,
            })
            .to_string();
        }
        g.running_day = Some(day.clone());
    }

    let (raw, pq, out, sh2, d2) = (
        a.raw_dir.clone(),
        a.parquet_dir.clone(),
        a.rollup_dir.clone(),
        Arc::clone(sh),
        day.clone(),
    );
    std::thread::spawn(move || {
        let t0 = std::time::Instant::now();
        let venues = ["kalshi", "polymarket", "polymarket_us"];
        let res = build_day(&raw, &pq, &out, &d2, DEFAULT_INTERVAL_NS, &venues);
        let secs = t0.elapsed().as_secs_f64();
        let value = match res {
            Ok(stats) => serde_json::json!({
                "day": d2,
                "ok": true,
                "elapsed_s": (secs * 10.0).round() / 10.0,
                "venues": stats.iter().map(|(v, s)| serde_json::json!({
                    "venue": v, "events": s.events, "samples": s.samples,
                    "markets": s.markets, "gaps": s.gaps,
                    "not_synced": s.not_synced, "parse_failures": s.parse_failures,
                })).collect::<Vec<_>>(),
                "samples": stats.iter().map(|(_, s)| s.samples).sum::<u64>(),
                "events": stats.iter().map(|(_, s)| s.events).sum::<u64>(),
            }),
            Err(e) => serde_json::json!({ "day": d2, "ok": false, "error": e,
                                          "elapsed_s": (secs * 10.0).round() / 10.0 }),
        };
        let mut g = sh2.lock().unwrap_or_else(|e| e.into_inner());
        g.running_day = None;
        g.last = Some(value);
    });

    serde_json::json!({ "started": true, "day": day }).to_string()
}

fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Option<T> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// Rebuild the books from venue snapshots on every request. The whole import is
/// ~250 entries and takes microseconds; holding no state means the page can
/// never show a number the files no longer support.
fn books_json(a: &Args) -> String {
    let mut cx = Cx::default();
    let mut j = Journal::new();

    let deposits: Vec<Deposit> =
        read_json(&format!("{}/kalshi_deposits.json", a.kalshi_dir)).unwrap_or_default();
    let fills: Vec<Fill> =
        read_json(&format!("{}/kalshi_fills.json", a.kalshi_dir)).unwrap_or_default();
    let settlements: Vec<Settlement> =
        read_json(&format!("{}/kalshi_settlements.json", a.kalshi_dir)).unwrap_or_default();
    if let Err(e) = (KalshiImport { deposits, fills, settlements }).apply(&mut cx, &mut j) {
        return format!("{{\"error\":\"kalshi import: {e}\"}}");
    }

    let mut pmus_buying_power: Option<String> = None;
    if !a.pmus_dir.is_empty() {
        if let Some(balances) =
            read_json::<Balances>(&format!("{}/pmus_balances.json", a.pmus_dir))
        {
            pmus_buying_power = Some(balances.buying_power_str());
            let positions: Vec<Position> =
                read_json(&format!("{}/pmus_positions.json", a.pmus_dir)).unwrap_or_default();
            if let Err(e) =
                (PmusImport { deposits_usd: a.pmus_deposits.clone(), balances, positions })
                    .apply(&mut cx, &mut j)
            {
                return format!("{{\"error\":\"pmus import: {e}\"}}");
            }
        }
    }

    let mut rep = report::build(&mut cx, &j);
    if let Some(kb) = &a.kalshi_balance {
        rep.reconciliations
            .push(report::reconcile(&mut cx, &j, accounts::CASH_KALSHI, kb));
    }
    if let Some(bp) = &pmus_buying_power {
        rep.reconciliations
            .push(report::reconcile(&mut cx, &j, accounts::CASH_PMUS, bp));
    }
    serde_json::to_string(&rep).unwrap_or_else(|_| "{}".into())
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.replace("%3A", ":").replace('+', " "));
            }
        }
    }
    None
}

/// Opportunity coverage per vetted pair, read straight off the tape —
/// Parquet for closed days, today's JSONL for the live one, resolved by
/// `arb_query::source_for`. Timed and reported so a slow range is visible
/// rather than mysterious.
fn opps_json(a: &Args, query: &str) -> String {
    let to = query_param(query, "to").unwrap_or_else(|| integrity::build(&a.data_dir).today);
    let from = query_param(query, "from").unwrap_or_else(|| to.clone());
    let rel = query_param(query, "rel");

    let t0 = std::time::Instant::now();
    let sources = sources_for_range(&a.scan_dir, &a.parquet_dir, "opportunities", &from, &to);
    let n_sources = sources.len();
    match opps::summarize(&sources, rel.as_deref()) {
        Ok(rows) => {
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            let obs: u64 = rows.iter().map(|r| r.observations).sum();
            format!(
                "{{\"from\":\"{from}\",\"to\":\"{to}\",\"days\":{n_sources},\
                 \"relationships\":{},\"observations\":{obs},\"query_ms\":{ms:.1},\
                 \"rows\":{}}}",
                rows.len(),
                serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into())
            )
        }
        Err(e) => format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    }
}

/// What the engine would have RESTING right now.
///
/// This is the answer to "what would we be trading if trading were enabled" —
/// the dry-run `arb-trader` runs the real quoter against the live recorder
/// socket and appends its intents here. Where this disagrees with the scenario
/// view, THIS is authoritative: that one is a calculator, this is the engine.
///
/// The age of the last intent is reported prominently because a stale file
/// looks exactly like a quiet market unless you say which it is.
fn intents_json(a: &Args) -> String {
    let st = match intents::reconstruct(&a.intents_path) {
        Ok(s) => s,
        Err(e) => {
            return serde_json::json!({
                "error": e,
                "hint": "start the dry-run engine: arb-trader --socket data/arbbot.sock \
                         --registry config/registry.yaml --out <this path>",
            })
            .to_string()
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let age = if st.last_ts > 0.0 { now - st.last_ts } else { -1.0 };
    let mut v = serde_json::to_value(&st).unwrap_or_else(|_| serde_json::json!({}));
    v["last_intent_age_s"] = serde_json::json!(age.round() as i64);
    // 120s is generous for a quoter watching three live feeds; beyond that the
    // engine is almost certainly not running.
    v["engine_live"] = serde_json::json!(age >= 0.0 && age < 120.0);
    v["path"] = serde_json::json!(a.intents_path);
    v.to_string()
}

/// The register of tracked pairs. Distinguishes DETECTED from PERMITTED: a
/// pair can carry the best edge on the board and still be untradable because
/// only a human verdict opens the gate (registry/model.py:132).
fn pairs_json(a: &Args) -> String {
    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);
    let rows: Vec<serde_json::Value> = reg
        .relationships
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "kind": r.kind,
                "verdict": r.verdict,
                "vetted_by": r.vetted_by,
                "tradable": r.tradable(&allow),
                "human_vetted": r.human_vetted(),
                "oracle_risk": r.oracle_risk,
                "tranche": r.tranche,
                "legs": r.legs.iter().map(|l| format!("{}:{}", l.venue, l.market_id))
                        .collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({
        "count": rows.len(),
        "tradable": reg.relationships.iter().filter(|r| r.tradable(&allow)).count(),
        "human_vetted": reg.relationships.iter().filter(|r| r.human_vetted()).count(),
        "allowlisted": allow.len(),
        "rows": rows,
    })
    .to_string()
}

/// One pair, end to end: what we asserted about it, both legs' quotes over
/// time, and the opportunity the scanner logged. The chart's load-bearing
/// series is `basket_cost` — below 1.00 is an arb before fees, so the arb is
/// visible as an AREA rather than something you infer from two lines.
fn pair_json(a: &Args, query: &str) -> String {
    let Some(rel_id) = query_param(query, "rel") else {
        return "{\"error\":\"rel is required\"}".into();
    };
    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let Some(r) = reg.get(&rel_id) else {
        return format!("{{\"error\":\"unknown relationship {rel_id}\"}}");
    };
    if r.legs.len() < 2 {
        return format!("{{\"error\":\"{rel_id} has {} leg(s)\"}}", r.legs.len());
    }
    let to = query_param(query, "to").unwrap_or_else(|| integrity::build(&a.data_dir).today);
    let from = query_param(query, "from").unwrap_or_else(|| to.clone());

    // ToB files are per venue per day; collect the days in range that exist.
    let days = day_range(&from, &to);
    let paths = |venue: &str| -> Vec<String> {
        days.iter()
            .map(|d| format!("{}/tob-{venue}-{d}.jsonl", a.rollup_dir))
            .filter(|p| std::path::Path::new(p).is_file())
            .collect()
    };
    let (la, lb) = (&r.legs[0], &r.legs[1]);
    let sa = series::load_market(&paths(&la.venue), &la.venue, &la.market_id);
    let sb = series::load_market(&paths(&lb.venue), &lb.venue, &lb.market_id);
    let points = series::align(&sa, &sb);
    // A basket cost is only believable if BOTH legs quoted recently. Anything
    // older is forward-fill: a price that was never simultaneously available.
    const FRESH_NS: i64 = 300_000_000_000; // 5 min
    let priced = points.iter().filter(|p| p.basket_cost.is_some()).count();
    let fresh: Vec<&series::AlignedPoint> =
        points.iter().filter(|p| p.is_fresh(FRESH_NS) && p.basket_cost.is_some()).collect();
    let fresh_under_1 = fresh.iter().filter(|p| p.basket_cost.unwrap() < 1.0).count();
    let best_fresh = fresh
        .iter()
        .filter_map(|p| p.basket_cost)
        .fold(f64::INFINITY, f64::min);

    let sources = sources_for_range(&a.scan_dir, &a.parquet_dir, "opportunities", &from, &to);
    let opp = opps::summarize(&sources, Some(&rel_id)).unwrap_or_default();

    serde_json::json!({
        "from": from, "to": to,
        "relationship": r,
        "tradable": r.tradable(&Allowlist::load(&a.tradable)),
        "human_vetted": r.human_vetted(),
        "leg_a": format!("{}:{}", la.venue, la.market_id),
        "leg_b": format!("{}:{}", lb.venue, lb.market_id),
        "samples_a": sa.len(),
        "samples_b": sb.len(),
        "points": points,
        "fresh_max_age_s": FRESH_NS / 1_000_000_000,
        "priced_points": priced,
        "fresh_points": fresh.len(),
        "fresh_points_under_1": fresh_under_1,
        "best_fresh_cost": if best_fresh.is_finite() { Some(best_fresh) } else { None },
        "opportunity": opp.first(),
    })
    .to_string()
}

fn venue_of(s: &str) -> Option<Venue> {
    match s {
        "kalshi" => Some(Venue::Kalshi),
        "polymarket" => Some(Venue::Polymarket),
        "polymarket_us" => Some(Venue::PolymarketUs),
        _ => None,
    }
}

/// Top N baskets by edge under a chosen execution style.
///
/// Ranked on CURRENT quotes — the latest sample in the ToB rollup — so every
/// row carries its quote age. This is as fresh as the rollup, not as fresh as
/// the venue, and saying so is the difference between an instrument and a lie.
///
/// Every scenario is priced for every pair, so switching the view is a re-sort
/// rather than a re-read, and a pair that is unprofitable to take but
/// profitable to make is visible as such.
fn current_json(a: &Args, query: &str) -> String {
    let scenario = query_param(query, "scenario")
        .and_then(|s| Scenario::parse(&s))
        .unwrap_or(Scenario::TakeTake);
    let clip_s = query_param(query, "clip").unwrap_or_else(|| "25".into());
    let n: usize = query_param(query, "n").and_then(|s| s.parse().ok()).unwrap_or(25);
    let day = query_param(query, "day").unwrap_or_else(|| integrity::build(&a.data_dir).today);
    // Default to the permitted universe. Detected-but-not-permitted edge is
    // genuinely worth seeing (it is the case for vetting a pair), but it should
    // be asked for, not ranked first by default.
    let only_tradable = query_param(query, "all").as_deref() != Some("1");

    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);

    let paths: Vec<String> = ["kalshi", "polymarket", "polymarket_us"]
        .iter()
        .map(|v| format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir))
        .filter(|p| std::path::Path::new(p).is_file())
        .collect();
    if paths.is_empty() {
        return format!(
            "{{\"error\":\"no ToB rollup for {day} — run arb-tob --day {day}\"}}"
        );
    }
    let latest = series::latest_by_market(&paths);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let clip = cx.parse_exact(&clip_s);
    let max_spread_s = query_param(query, "max_spread").unwrap_or_else(|| "0.05".into());
    let max_spread = cx.parse_exact(&max_spread_s);

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for r in &reg.relationships {
        if r.legs.len() != 2 {
            continue;
        }
        let (la, lb) = (&r.legs[0], &r.legs[1]);
        let (Some(va), Some(vb)) = (venue_of(&la.venue), venue_of(&lb.venue)) else { continue };
        let ka = (la.venue.clone(), la.market_id.clone());
        let kb = (lb.venue.clone(), lb.market_id.clone());
        let (Some(sa), Some(sb)) = (latest.get(&ka), latest.get(&kb)) else { continue };
        let qa = Quote { bid: sa.bid.clone(), ask: sa.ask.clone() };
        let qb = Quote { bid: sb.bid.clone(), ask: sb.ask.clone() };

        let mut by_scenario = serde_json::Map::new();
        let mut chosen: Option<arb_scenario::Priced> = None;
        for sc in Scenario::all() {
            if let Some(p) =
                price(&mut cx, &sched, sc, va, vb, &qa, &qb, clip, "politics", max_spread)
            {
                if sc == scenario {
                    chosen = Some(p.clone());
                }
                by_scenario.insert(sc.as_str().into(), serde_json::to_value(&p).unwrap());
            }
        }
        let Some(p) = chosen else { continue };
        let tradable = r.tradable(&allow);
        if only_tradable && !tradable {
            continue;
        }
        rows.push(serde_json::json!({
            "relationship_id": r.id,
            "tradable": tradable,
            "verdict": r.verdict,
            "leg_a": format!("{}:{}", la.venue, la.market_id),
            "leg_b": format!("{}:{}", lb.venue, lb.market_id),
            "quote_age_a_s": (now_ns - sa.ts_local_ns) / 1_000_000_000,
            "quote_age_b_s": (now_ns - sb.ts_local_ns) / 1_000_000_000,
            "edge_per_contract": p.edge_per_contract,
            "priced": p,
            "scenarios": by_scenario,
        }));
    }

    // Plausible fills first, THEN by edge. Sorting on raw edge alone puts
    // un-fillable wide-spread rests at the top, which is exactly backwards for
    // a view whose job is to say what to trade.
    rows.sort_by(|x, y| {
        let ok = |v: &serde_json::Value| v["priced"]["fill_plausible"].as_bool().unwrap_or(false);
        let f = |v: &serde_json::Value| {
            v["edge_per_contract"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::MIN)
        };
        ok(y).cmp(&ok(x)).then(f(y).partial_cmp(&f(x)).unwrap_or(std::cmp::Ordering::Equal))
    });
    let total = rows.len();
    let profitable = rows
        .iter()
        .filter(|v| v["priced"]["profitable"].as_bool().unwrap_or(false))
        .count();
    let actionable = rows
        .iter()
        .filter(|v| {
            v["priced"]["profitable"].as_bool().unwrap_or(false)
                && v["priced"]["fill_plausible"].as_bool().unwrap_or(false)
        })
        .count();
    rows.truncate(n);

    serde_json::json!({
        "day": day,
        "scenario": scenario.as_str(),
        "clip": clip_s,
        "priced_pairs": total,
        "profitable": profitable,
        "actionable": actionable,
        "max_spread": max_spread_s,
        "only_tradable": only_tradable,
        "shown": rows.len(),
        "rows": rows,
    })
    .to_string()
}

/// Inclusive YYYY-MM-DD range. Capped so a careless URL cannot ask the server
/// to stat thousands of files.
fn day_range(from: &str, to: &str) -> Vec<String> {
    let parse = |s: &str| -> Option<(i32, u32, u32)> {
        let b = s.as_bytes();
        if s.len() != 10 || b[4] != b'-' || b[7] != b'-' {
            return None;
        }
        Some((s[0..4].parse().ok()?, s[5..7].parse().ok()?, s[8..10].parse().ok()?))
    };
    let (Some(f), Some(t)) = (parse(from), parse(to)) else { return vec![] };
    let to_days = |(y, m, d): (i32, u32, u32)| -> i64 {
        let (y, m) = if m <= 2 { (y - 1, m + 12) } else { (y, m) };
        let era = y.div_euclid(100) as i64;
        365 * y as i64 + y as i64 / 4 - y as i64 / 100 + y as i64 / 400
            + (153 * (m as i64 - 3) + 2) / 5
            + d as i64
            - era * 0
    };
    let (a0, b0) = (to_days(f), to_days(t));
    if b0 < a0 || b0 - a0 > 400 {
        return vec![];
    }
    // Walk calendar days without a date library: increment and normalise.
    let mut out = Vec::new();
    let (mut y, mut m, mut d) = f;
    for _ in 0..=(b0 - a0) {
        out.push(format!("{y:04}-{m:02}-{d:02}"));
        d += 1;
        let dim = match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                    29
                } else {
                    28
                }
            }
        };
        if d > dim {
            d = 1;
            m += 1;
            if m > 12 {
                m = 1;
                y += 1;
            }
        }
    }
    out
}

fn respond(mut s: TcpStream, status: &str, ctype: &str, body: &str) {
    let out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = s.write_all(out.as_bytes());
    let _ = s.flush();
}

fn handle(s: TcpStream, a: &Args, sh: &Shared) {
    let mut line = String::new();
    {
        let mut r = BufReader::new(match s.try_clone() {
            Ok(c) => c,
            Err(_) => return,
        });
        if r.read_line(&mut line).is_err() {
            return;
        }
    }
    let method = line.split_whitespace().next().unwrap_or("GET").to_string();
    let full = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let path = full.split('?').next().unwrap_or("/");
    let query = full.split_once('?').map(|(_, q)| q.to_string()).unwrap_or_default();
    if path.starts_with("/pair/") {
        respond(s, "200 OK", "text/html; charset=utf-8", PAGE);
        return;
    }
    match path {
        // Every view is a real URL. The shell is the same document; its
        // router picks the view from the path and fetches ONLY that view's
        // endpoints, which is the point — a single page would fan out to
        // every endpoint on every load as views are added.
        "/" | "/recording" | "/opportunities" | "/pairs" | "/current" | "/intents" => {
            respond(s, "200 OK", "text/html; charset=utf-8", PAGE)
        }
        "/api/books" => respond(s, "200 OK", "application/json", &books_json(a)),
        "/api/integrity" => {
            let i = integrity::build(&a.data_dir);
            let body = serde_json::to_string(&i).unwrap_or_else(|_| "{}".into());
            respond(s, "200 OK", "application/json", &body)
        }
        "/api/opportunities" => respond(s, "200 OK", "application/json", &opps_json(a, &query)),
        "/api/pairs" => respond(s, "200 OK", "application/json", &pairs_json(a)),
        "/api/intents" => respond(s, "200 OK", "application/json", &intents_json(a)),
        // The single write surface. GET reports status; only POST can start a
        // build, so nothing fires it by merely loading a page.
        "/api/rollup" => {
            if method == "POST" {
                respond(s, "200 OK", "application/json", &rollup_start(a, sh, &query))
            } else {
                respond(s, "200 OK", "application/json", &rollup_status(sh))
            }
        }
        "/api/current" => respond(s, "200 OK", "application/json", &current_json(a, &query)),
        "/api/pair" => respond(s, "200 OK", "application/json", &pair_json(a, &query)),
        _ => respond(s, "404 Not Found", "text/plain", "not found"),
    }
}

fn main() {
    let mut a = Args {
        kalshi_dir: String::new(),
        pmus_dir: String::new(),
        pmus_deposits: String::new(),
        kalshi_balance: None,
        data_dir: "data".into(),
        scan_dir: "data/scan".into(),
        raw_dir: "data/raw".into(),
        parquet_dir: "data/parquet".into(),
        rollup_dir: "data/rollup".into(),
        intents_path: "data/trader-rs/intents.jsonl".into(),
        registry: "config/registry.yaml".into(),
        tradable: "config/tradable.yaml".into(),
        port: 4749,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let v = args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--kalshi-dir" => a.kalshi_dir = v,
            "--pmus-dir" => a.pmus_dir = v,
            "--pmus-deposits" => a.pmus_deposits = v,
            "--kalshi-balance" => a.kalshi_balance = Some(v),
            "--data-dir" => a.data_dir = v,
            "--scan-dir" => a.scan_dir = v,
            "--raw-dir" => a.raw_dir = v,
            "--parquet-dir" => a.parquet_dir = v,
            "--rollup-dir" => a.rollup_dir = v,
            "--intents" => a.intents_path = v,
            "--registry" => a.registry = v,
            "--tradable" => a.tradable = v,
            "--port" => a.port = v.parse().unwrap_or(4749),
            other => {
                eprintln!("unknown arg: {other}");
                exit(2);
            }
        }
        i += 2;
    }
    if a.kalshi_dir.is_empty() {
        eprintln!("--kalshi-dir is required");
        exit(2);
    }

    let addr = format!("127.0.0.1:{}", a.port);
    let l = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot bind {addr}: {e}");
            exit(1);
        }
    };
    println!("arb-dash on http://{addr}  (read-only, 127.0.0.1 only)");
    let shared: Shared = Arc::new(Mutex::new(Rollup::default()));
    for s in l.incoming().flatten() {
        handle(s, &a, &shared);
    }
}
