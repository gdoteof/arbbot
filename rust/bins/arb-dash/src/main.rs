//! arb-dash — read-only instrument over the double-entry books.
//!
//! Binds 127.0.0.1 only, reads local files, never writes and never touches a
//! venue order path. Deliberately runs on a DIFFERENT port from the Python
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
use std::process::exit;

use arb_core::scan::Cx;
use arb_ledger::kalshi::{Deposit, Fill, KalshiImport, Settlement};
use arb_ledger::pmus::{Balances, PmusImport, Position};
use arb_ledger::{accounts, report, Journal};
use arb_query::{opps, sources_for_range};
use arb_registry::{Allowlist, Registry};
use arb_tob::series;

const PAGE: &str = include_str!("index.html");

struct Args {
    kalshi_dir: String,
    pmus_dir: String,
    pmus_deposits: String,
    kalshi_balance: Option<String>,
    data_dir: String,
    scan_dir: String,
    parquet_dir: String,
    rollup_dir: String,
    registry: String,
    tradable: String,
    port: u16,
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

fn handle(s: TcpStream, a: &Args) {
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
        "/" | "/recording" | "/opportunities" | "/pairs" => {
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
        parquet_dir: "data/parquet".into(),
        rollup_dir: "data/rollup".into(),
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
            "--parquet-dir" => a.parquet_dir = v,
            "--rollup-dir" => a.rollup_dir = v,
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
    for s in l.incoming().flatten() {
        handle(s, &a);
    }
}
