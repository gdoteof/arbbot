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
//! Updates are PUSHED, not polled: `GET /api/stream` is a Server-Sent Events
//! connection that emits one line whenever the files a view reads actually
//! change. An idle dashboard therefore makes zero requests, and a change shows
//! up in about a second instead of on the next timer tick. SSE rather than a
//! WebSocket because the traffic is one-directional and SSE is plain HTTP —
//! no handshake, no framing, no dependency, and the browser reconnects on its
//! own. That connection is long-lived, so the accept loop serves each
//! connection on its own thread; serially, one stream would wedge the server.
//!
//!   arb-dash --kalshi-dir <dir> --pmus-dir <dir> --pmus-deposits <usd> \
//!            [--data-dir data] [--port 4749] [--kalshi-balance <usd>]

mod integrity;
mod trades;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::process::exit;
use std::time::{Duration, UNIX_EPOCH};

use arb_core::fees::FeeSchedule;
use arb_core::model::Venue;
use arb_core::scan::Cx;
use arb_ledger::kalshi::{Deposit, Fill, KalshiImport, Settlement};
use arb_ledger::pmus::{Balances, PmusImport, Position};
use arb_ledger::{accounts, report, Journal};
use arb_query::{intents, opps, sources_for_range};
use arb_registry::{Allowlist, Registry};
use arb_core::fees::Role;
use arb_scenario::{price, price_at, Quote, Scenario};
use arb_tob::{build_day, series, DEFAULT_INTERVAL_NS};

const PAGE: &str = include_str!("index.html");

/// Polymarket INTERNATIONAL charges a taker fee that depends on the market's
/// category: 0.00 geopolitics, 0.04 politics/finance/tech, 0.05
/// sports/economics/culture/weather, 0.07 crypto. The scanner reads that
/// category off the market metadata; this dashboard does not have it, and was
/// hardcoding "politics" — which is not a neutral guess. On the 43 registry
/// pairs with an intl leg it OVERSTATES edge on the crypto ones (btc-1m,
/// btc-dip-10k) and the sports/weather ones (f1, earthquake, volcano, meteor),
/// and UNDERSTATES it on the geopolitics ones (taiwan, iran, nato, putin).
///
/// Priced at the documented default instead, which is the same 0.04 but says
/// what it is, and rows with an intl leg are flagged so the number is read
/// with that caveat. The real fix is plumbing per-market category through to
/// the dashboard.
const FEE_CATEGORY: &str = "default";

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
    /// Append-only trade ledger. The Trades view derives every number from
    /// this file and stores nothing, so it cannot disagree with what the
    /// engine booked.
    ledger_path: String,
    registry: String,
    tradable: String,
    port: u16,
    /// Epoch seconds at startup — the age of anything passed on the command
    /// line rather than read from a file.
    started_at: u64,
}

/// The one piece of mutable state in the process, and it exists only so a
/// second trigger cannot start while a build is in flight.
#[derive(Default)]
struct Rollup {
    running_day: Option<String>,
    last: Option<serde_json::Value>,
}

type Shared = Arc<Mutex<Rollup>>;

/// Status of the rollup, including the part that outlives this process. The
/// old version reported only builds from THIS session, so a fresh restart said
/// "no build this session" over a series that had been on disk since noon —
/// and a completed build showed its duration but never when it finished. The
/// series is written atomically at the end of a build, so the file's mtime IS
/// the build's completion time.
fn rollup_status(a: &Args, sh: &Shared) -> String {
    let g = sh.lock().unwrap_or_else(|e| e.into_inner());
    let day = integrity::build(&a.data_dir).today;
    let built_age_s = ["kalshi", "polymarket", "polymarket_us"]
        .iter()
        .filter_map(|v| age_secs(&format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir)))
        .max();
    serde_json::json!({
        "running": g.running_day,
        "last": g.last,
        "day": day,
        "on_disk": built_age_s.is_some(),
        "built_age_s": built_age_s,
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

/// Length and mtime of one file, folded into a number. Never reads content:
/// the point is to notice a change for the price of a `stat`, so the whole
/// fingerprint can be recomputed once a second forever.
fn stat_sig(path: &str) -> u64 {
    let Ok(m) = std::fs::metadata(path) else { return 0 };
    let mtime = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    m.len() ^ mtime.rotate_left(17)
}

fn stat_all(paths: &[String]) -> u64 {
    paths.iter().fold(0u64, |acc, p| acc.rotate_left(7) ^ stat_sig(p))
}

/// What each view depends on, one number per view, plus the rollup's own
/// status. The client re-renders ONLY the view whose number moved, which is
/// what makes a push cheaper than a poll: an idle board redraws nothing.
fn state_json(a: &Args, sh: &Shared) -> String {
    let day = integrity::build(&a.data_dir).today;
    let venues = ["kalshi", "polymarket", "polymarket_us"];
    let books = stat_all(&[
        format!("{}/kalshi_deposits.json", a.kalshi_dir),
        format!("{}/kalshi_fills.json", a.kalshi_dir),
        format!("{}/kalshi_settlements.json", a.kalshi_dir),
        format!("{}/pmus_balances.json", a.pmus_dir),
        format!("{}/pmus_positions.json", a.pmus_dir),
    ]);
    let recording = stat_all(
        &venues.iter().map(|v| format!("{}/raw/{v}-{day}.jsonl", a.data_dir)).collect::<Vec<_>>(),
    );
    let rollup = stat_all(
        &venues.iter().map(|v| format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir)).collect::<Vec<_>>(),
    );
    let intents = stat_sig(&a.intents_path);
    let opps = stat_sig(&format!("{}/opportunities-{day}.jsonl", a.scan_dir));
    let registry = stat_all(&[a.registry.clone(), a.tradable.clone()]);
    format!(
        "{{\"today\":\"{day}\",\"books\":{books},\"recording\":{recording},\
         \"rollup\":{rollup},\"intents\":{intents},\"opportunities\":{opps},\
         \"pairs\":{registry},\"rollup_status\":{}}}",
        rollup_status(a, sh)
    )
}

/// One SSE connection. Sends a line only when something moved, and a comment
/// heartbeat otherwise — which is also how a closed tab is detected, since the
/// write is the only thing that fails.
///
/// The live tapes grow every second, so "push on any change" would redraw the
/// recording and intents views continuously. Changes are therefore coalesced
/// into at most one frame every `MIN_PUSH`: idle still costs nothing, and a
/// moving market still shows up an order of magnitude sooner than the 15s
/// timer this replaced.
fn stream(mut s: TcpStream, a: &Args, sh: &Shared) {
    const MIN_PUSH: u32 = 3;
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                Cache-Control: no-store\r\nConnection: keep-alive\r\n\r\n";
    if s.write_all(head.as_bytes()).is_err() {
        return;
    }
    let mut last = String::new();
    let (mut tick, mut since_push) = (0u32, MIN_PUSH);
    loop {
        let body = state_json(a, sh);
        let out = if body != last && since_push >= MIN_PUSH {
            last = body.clone();
            since_push = 0;
            format!("data: {body}\n\n")
        } else if tick % 15 == 0 {
            ": ping\n\n".to_string()
        } else {
            String::new()
        };
        if !out.is_empty() && (s.write_all(out.as_bytes()).is_err() || s.flush().is_err()) {
            return;
        }
        tick = tick.wrapping_add(1);
        since_push = since_push.saturating_add(1);
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Append-only files worth watching live, newest-writer first.
///
/// Discovered rather than configured: any `*intents*.jsonl` beside the
/// configured intents file, plus the ledger. That covers the shadow engine and
/// every armed slice without a new flag each time one is added.
fn tape_sources(a: &Args) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let dir = std::path::Path::new(&a.intents_path)
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    if let Ok(rd) = std::fs::read_dir(&dir) {
        let mut names: Vec<String> = rd
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.contains("intents") && n.ends_with(".jsonl"))
            .collect();
        names.sort();
        for n in names {
            let label = n.trim_end_matches(".jsonl").to_string();
            out.push((label, dir.join(&n).to_string_lossy().to_string()));
        }
    }
    out.push(("ledger".into(), a.ledger_path.clone()));
    out
}

/// The live tape: every line these files gain, pushed as it lands.
///
/// This is deliberately NOT the `/api/stream` model. That one pushes a
/// signature of which files changed and the client refetches a whole view,
/// which costs a minimum of a few seconds and cannot show anything that is not
/// already a rendered panel. Watching an engine work needs the opposite: the
/// events themselves, in order, as soon as they exist.
///
/// Poll interval is short because the cost is one `metadata()` per file per
/// tick — the engine already flushes each line for exactly this reason.
fn tape(mut s: TcpStream, a: &Args) {
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                Cache-Control: no-store\r\nConnection: keep-alive\r\n\r\n";
    if s.write_all(head.as_bytes()).is_err() {
        return;
    }
    let srcs = tape_sources(a);
    // Start near the end: a tape that replays the whole day on every page load
    // would bury what is happening now under history. A short backlog keeps the
    // view from opening blank.
    //
    // Backlog only from files something is ACTIVELY writing. The trader-rs dir
    // also holds archived runs (intents-shadow-0724.jsonl), and seeding from
    // those replayed four-day-old quotes into a live tape — history dressed up
    // as news, which is worse than an empty view. Stale files are still
    // watched; they simply start at EOF and stay silent unless written again.
    const BACKLOG: u64 = 8 * 1024;
    const FRESH_S: u64 = 300;
    let mut at: Vec<u64> = srcs
        .iter()
        .map(|(_, p)| match std::fs::metadata(p) {
            Ok(m) => {
                let fresh = m
                    .modified()
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .map(|d| d.as_secs() < FRESH_S)
                    .unwrap_or(false);
                if fresh { m.len().saturating_sub(BACKLOG) } else { m.len() }
            }
            Err(_) => 0,
        })
        .collect();
    // A backlog cut lands mid-line; drop the first partial so the view never
    // opens on a torn record.
    let mut primed: Vec<bool> = srcs.iter().map(|_| false).collect();
    let mut tick: u64 = 0;
    loop {
        for (i, (label, path)) in srcs.iter().enumerate() {
            let len = match std::fs::metadata(path) {
                Ok(m) => m.len(),
                // Not there yet is normal: an armed slice writes its file only
                // once it starts. Keep watching.
                Err(_) => continue,
            };
            if len < at[i] {
                // truncated or rotated — follow the new file from its start
                at[i] = 0;
                primed[i] = true;
            }
            if len == at[i] {
                continue;
            }
            let Ok(mut f) = std::fs::File::open(path) else { continue };
            use std::io::{Read, Seek, SeekFrom};
            if f.seek(SeekFrom::Start(at[i])).is_err() {
                continue;
            }
            let mut buf = Vec::new();
            if f.take(1 << 20).read_to_end(&mut buf).is_err() {
                continue;
            }
            // Only consume through the last complete line; a partial tail stays
            // for the next tick rather than being pushed half-written.
            let end = match buf.iter().rposition(|b| *b == b'\n') {
                Some(p) => p + 1,
                None => continue,
            };
            at[i] += end as u64;
            let text = String::from_utf8_lossy(&buf[..end]).to_string();
            for (n, line) in text.lines().enumerate() {
                if !primed[i] && n == 0 {
                    continue; // the torn first line of a backlog seek
                }
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let parsed: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|_| serde_json::json!({ "raw": line }));
                // Classify here so the client can filter without parsing
                // shapes. Skips outnumber actions ~100:1 — the engine rejects
                // far more than it places — so a tape that cannot separate
                // them shows only budget messages and hides every real order.
                let kind = if parsed.get("skip").is_some() {
                    "skip"
                } else if parsed.get("hedge_needed").is_some() {
                    "hedge"
                } else if parsed.get("relationship_id").is_some() {
                    "basket"
                } else if parsed.get("cancel").is_some() {
                    "cancel"
                } else if parsed.get("tag").and_then(|t| t.as_str()) == Some("take-take") {
                    "take-take"
                } else if parsed.get("place").is_some() {
                    "place"
                } else {
                    "other"
                };
                let frame = serde_json::json!({"src": label, "kind": kind, "ev": parsed});
                if s.write_all(format!("data: {frame}\n\n").as_bytes()).is_err() {
                    return;
                }
            }
            primed[i] = true;
        }
        if s.flush().is_err() {
            return;
        }
        // Keepalive so an idle engine does not look like a dead connection.
        if tick % 100 == 0 && s.write_all(b": ping\n\n").is_err() {
            return;
        }
        tick = tick.wrapping_add(1);
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// A missing or unparseable snapshot is an ERROR, never an empty list. Books
/// built from files that are not there balance perfectly and mean nothing: the
/// old `unwrap_or_default()` turned "the fills file is gone" into a clean,
/// zeroed, trial-balance-ZERO statement — the most convincing possible way to
/// show numbers that are not true.
fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))
}

fn age_secs(path: &str) -> Option<u64> {
    let m = std::fs::metadata(path).ok()?;
    let mtime = m.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(now_secs().saturating_sub(mtime))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Rebuild the books from venue snapshots on every request. The whole import is
/// ~250 entries and takes microseconds; holding no state means the page can
/// never show a number the files no longer support.
fn books_json(a: &Args) -> String {
    let mut cx = Cx::default();
    let mut j = Journal::new();

    let kd = format!("{}/kalshi_deposits.json", a.kalshi_dir);
    let kf = format!("{}/kalshi_fills.json", a.kalshi_dir);
    let ks = format!("{}/kalshi_settlements.json", a.kalshi_dir);
    let (deposits, fills, settlements): (Vec<Deposit>, Vec<Fill>, Vec<Settlement>) =
        match (read_json(&kd), read_json(&kf), read_json(&ks)) {
            (Ok(d), Ok(f), Ok(s)) => (d, f, s),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                return format!("{{\"error\":\"kalshi snapshot unreadable — {}\"}}",
                               e.replace('"', "'"))
            }
        };
    if let Err(e) = (KalshiImport { deposits, fills, settlements }).apply(&mut cx, &mut j) {
        return format!("{{\"error\":\"kalshi import: {e}\"}}");
    }

    // Provenance, one row per number the reconciliation leans on.
    let mut sources = vec![
        serde_json::json!({ "what": "kalshi fills", "from": kf, "age_s": age_secs(&kf) }),
        serde_json::json!({ "what": "kalshi settlements", "from": ks, "age_s": age_secs(&ks) }),
        serde_json::json!({ "what": "kalshi deposits", "from": kd, "age_s": age_secs(&kd) }),
    ];

    let mut pmus_buying_power: Option<String> = None;
    if !a.pmus_dir.is_empty() {
        let pb = format!("{}/pmus_balances.json", a.pmus_dir);
        let pp = format!("{}/pmus_positions.json", a.pmus_dir);
        let (balances, positions): (Balances, Vec<Position>) =
            match (read_json(&pb), read_json(&pp)) {
                (Ok(b), Ok(p)) => (b, p),
                (Err(e), _) | (_, Err(e)) => {
                    // Silently skipping this used to drop half the portfolio
                    // and its reconciliation row, leaving a statement that
                    // looked complete.
                    return format!("{{\"error\":\"pmus snapshot unreadable — {}\"}}",
                                   e.replace('"', "'"))
                }
            };
        pmus_buying_power = Some(balances.buying_power_str());
        sources.push(serde_json::json!({ "what": "pm-us balances", "from": pb,
                                         "age_s": age_secs(&pb) }));
        sources.push(serde_json::json!({ "what": "pm-us positions", "from": pp,
                                         "age_s": age_secs(&pp) }));
        if let Err(e) =
            (PmusImport { deposits_usd: a.pmus_deposits.clone(), balances, positions })
                .apply(&mut cx, &mut j)
        {
            return format!("{{\"error\":\"pmus import: {e}\"}}");
        }
    }

    let mut rep = report::build(&mut cx, &j);
    if let Some(kb) = &a.kalshi_balance {
        rep.reconciliations
            .push(report::reconcile(&mut cx, &j, accounts::CASH_KALSHI, kb));
        // NOT a venue read. A number typed on the command line cannot drift,
        // so this row reads EXACT forever whatever the account does.
        sources.push(serde_json::json!({
            "what": "kalshi cash balance",
            "from": "--kalshi-balance (fixed at startup)",
            "age_s": now_secs().saturating_sub(a.started_at),
            "frozen": true,
        }));
    }
    if let Some(bp) = &pmus_buying_power {
        rep.reconciliations
            .push(report::reconcile(&mut cx, &j, accounts::CASH_PMUS, bp));
    }
    sources.push(serde_json::json!({
        "what": "pm-us capital in",
        "from": "--pmus-deposits (fixed at startup)",
        "age_s": now_secs().saturating_sub(a.started_at),
        "frozen": true,
    }));

    let mut v = serde_json::to_value(&rep).unwrap_or_else(|_| serde_json::json!({}));
    v["sources"] = serde_json::Value::Array(sources);
    v.to_string()
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

/// What the engine would have RESTING right now, joined to the pair it belongs
/// to and priced with fees broken out.
///
/// The intent stream is MAKER-ONLY: place, reprice, cancel. A take-take
/// crossing is an immediate execution, never a resting quote, so it cannot
/// appear here by construction — that lives in the scenario view. What the
/// stream DOES tell us is which legs we are resting on, and that is the
/// execution style:
///   resting on leg A only  -> make A, take B when it fills
///   resting on leg B only  -> take A, make B
///   resting on both        -> make/make (no taking at all)
///
/// The take leg is priced off the venue quote from the ToB rollup, so its age
/// is reported per row: a stale quote makes the edge a guess, not a number.
/// Trades this system made, priced. Everything is derived from the ledger on
/// each request — no cache, so the tab cannot show a number the ledger does
/// not support.
fn trades_json(a: &Args) -> String {
    let text = match std::fs::read_to_string(&a.ledger_path) {
        Ok(t) => t,
        // A missing ledger is the NORMAL state before the engine has ever been
        // armed, so it must read as an empty book rather than an error.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return serde_json::json!({
                "error": format!("read {}: {e}", a.ledger_path),
            })
            .to_string()
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    trades::build(&text, FEE_CATEGORY, now).to_string()
}

fn intents_json(a: &Args) -> String {
    let text = match std::fs::read_to_string(&a.intents_path) {
        Ok(t) => t,
        Err(e) => {
            return serde_json::json!({
                "error": format!("read {}: {e}", a.intents_path),
                "hint": "start the dry-run engine: systemctl --user start arbbot-trader-rs",
            })
            .to_string()
        }
    };
    let st = intents::fold(&text);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let age = if st.last_ts > 0.0 { now - st.last_ts } else { -1.0 };

    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);
    // market -> relationship. A market can back more than one pair; the first
    // is enough to name the row, and the leg lookup below is exact anyway.
    let mut by_market: HashMap<(String, String), String> = HashMap::new();
    for r in &reg.relationships {
        if r.legs.len() != 2 {
            continue;
        }
        for l in &r.legs {
            by_market
                .entry((l.venue.clone(), l.market_id.clone()))
                .or_insert_with(|| r.id.clone());
        }
    }

    // Our resting quote per (venue, market, side).
    let mut ours: HashMap<(String, String, String), &intents::LiveOrder> = HashMap::new();
    let mut rels: Vec<String> = Vec::new();
    for o in &st.live {
        if let Some(rel) = by_market.get(&(o.venue.clone(), o.market.clone())) {
            if !rels.contains(rel) {
                rels.push(rel.clone());
            }
        }
        ours.insert((o.venue.clone(), o.market.clone(), o.side.clone()), o);
    }

    let day = integrity::build(&a.data_dir).today;
    let paths: Vec<String> = ["kalshi", "polymarket", "polymarket_us"]
        .iter()
        .map(|v| format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir))
        .filter(|p| std::path::Path::new(p).is_file())
        .collect();
    let latest = series::latest_by_market(&paths);
    // How far the ROLLUP covers, not when an individual market last moved.
    //
    // This distinction cost me a wrong headline. The series emits on CHANGE,
    // so a sample from 13 hours ago on a market that has not moved IS the
    // current book. Treating per-market sample age as staleness conflates
    // "quiet" with "unknown" and reported 0 of 67 pairs as actionable when 47
    // had a perfectly fillable book (median spread 2c). What actually goes
    // stale is the rollup itself: if it was built at noon, nothing in it knows
    // about the afternoon.
    let coverage_ns = latest.values().map(|s| s.ts_local_ns).max().unwrap_or(0);
    let coverage_age_s = if coverage_ns > 0 {
        ((now * 1e9) as i64 - coverage_ns) / 1_000_000_000
    } else {
        i64::MAX
    };
    const MAX_COVERAGE_AGE_S: i64 = 1800;
    let rollup_current = coverage_age_s <= MAX_COVERAGE_AGE_S;

    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let clip = cx.parse_exact("25");

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for rel_id in rels {
        let Some(r) = reg.get(&rel_id) else { continue };
        let (la, lb) = (&r.legs[0], &r.legs[1]);
        let (Some(va), Some(vb)) = (venue_of(&la.venue), venue_of(&lb.venue)) else { continue };

        let get = |l: &arb_registry::Leg, side: &str| {
            ours.get(&(l.venue.clone(), l.market_id.clone(), side.to_string())).copied()
        };
        // Buying YES on A means resting a BID; selling YES on B means resting an ASK.
        let rest_a = get(la, "bid");
        let rest_b = get(lb, "ask");
        if rest_a.is_none() && rest_b.is_none() {
            continue;
        }
        let qa = latest.get(&(la.venue.clone(), la.market_id.clone()));
        let qb = latest.get(&(lb.venue.clone(), lb.market_id.clone()));

        // ROUTES, not a single style. Resting on both legs is not "make/make
        // hoping both fill" — it is TWO make-take entry points: whichever side
        // gets hit, we immediately take the other. So price every route the
        // pair currently supports and rank on the best one.
        //
        //   take-take      cross both books now (no resting needed)
        //   make-a-take-b  our bid on A fills -> take B at its bid
        //   take-a-make-b  our ask on B fills -> take A at its ask
        let mut routes = serde_json::Map::new();
        let mut best: Option<(f64, &'static str)> = None;
        let mut consider = |label: &'static str,
                            ra: Role, rb: Role,
                            ea: Option<String>, eb: Option<String>,
                            cx: &mut Cx,
                            routes: &mut serde_json::Map<String, serde_json::Value>,
                            best: &mut Option<(f64, &'static str)>| {
            let (Some(ea), Some(eb)) = (ea, eb) else { return };
            let Some(p) = price_at(cx, &sched, label, va, vb, ra, rb, &ea, &eb, clip, FEE_CATEGORY)
            else {
                return;
            };
            if let Ok(e) = p.edge_per_contract.parse::<f64>() {
                if best.map_or(true, |(b, _)| e > b) {
                    *best = Some((e, label));
                }
            }
            routes.insert(label.into(), serde_json::to_value(&p).unwrap());
        };

        let va_ask = qa.and_then(|s| s.ask.clone());
        let vb_bid = qb.and_then(|s| s.bid.clone());
        // the immediate crossing — always available if both books are known
        consider("take-take", Role::Taker, Role::Taker,
                 va_ask.clone(), vb_bid.clone(), &mut cx, &mut routes, &mut best);
        // our resting bid on A fills, then we take B
        if let Some(o) = rest_a {
            consider("make-a-take-b", Role::Maker, Role::Taker,
                     Some(o.price.clone()), vb_bid.clone(), &mut cx, &mut routes, &mut best);
        }
        // our resting ask on B fills, then we take A
        if let Some(o) = rest_b {
            consider("take-a-make-b", Role::Taker, Role::Maker,
                     va_ask.clone(), Some(o.price.clone()), &mut cx, &mut routes, &mut best);
        }
        if routes.is_empty() {
            continue;
        }
        let (best_edge, best_route) = best.unwrap_or((f64::MIN, "none"));
        let priced = routes.get(best_route).cloned();

        // Fill plausibility applies to the leg we REST on, and only for the
        // make-take routes; a take-take needs no rest at all.
        let q_age = |s: Option<&arb_tob::TobSample>| {
            s.map(|s| ((now * 1e9) as i64 - s.ts_local_ns) / 1_000_000_000)
        };
        let spread_of = |s: Option<&arb_tob::TobSample>| -> Option<f64> {
            let s = s?;
            Some(s.ask.as_ref()?.parse::<f64>().ok()? - s.bid.as_ref()?.parse::<f64>().ok()?)
        };
        let rest_spread: Option<f64> = match best_route {
            "make-a-take-b" => spread_of(qa),
            "take-a-make-b" => spread_of(qb),
            _ => None,
        };
        const MAX_REST_SPREAD: f64 = 0.05;
        let fillable = rest_spread.map_or(true, |s| s <= MAX_REST_SPREAD);
        let have_books = qa.is_some() && qb.is_some();

        // Depth at the touch on the legs this route CROSSES. A rested leg
        // needs none — we are the size there — but a taken leg is only good
        // for what is actually resting behind the price. `None` means the
        // tape carried no size for a leg we would have to cross, which is not
        // permission to assume it is deep.
        let sz = |s: Option<&arb_tob::TobSample>, ask: bool| -> Option<f64> {
            let s = s?;
            let v = if ask { s.ask_size.as_ref() } else { s.bid_size.as_ref() };
            v?.parse::<f64>().ok()
        };
        let takes: &[Option<f64>] = match best_route {
            "take-take" => &[sz(qa, true), sz(qb, false)],
            "make-a-take-b" => &[sz(qb, false)],
            "take-a-make-b" => &[sz(qa, true)],
            _ => &[],
        };
        let take_depth = takes
            .iter()
            .try_fold(f64::INFINITY, |m: f64, d| d.map(|d| m.min(d)))
            .filter(|d| d.is_finite());
        const CLIP: f64 = 25.0;
        let deep_enough = take_depth.is_some_and(|d| d >= CLIP);

        let actionable =
            fillable && deep_enough && have_books && rollup_current && best_edge > 0.0;

        rows.push(serde_json::json!({
            "relationship_id": rel_id,
            "tradable": r.tradable(&allow),
            // Polymarket international's taker fee is category-dependent and we
            // price it at the default; see FEE_CATEGORY.
            "intl_leg": la.venue == "polymarket" || lb.venue == "polymarket",
            "best_route": best_route,
            "routes": routes,
            "leg_a": {
                "venue": la.venue, "market": la.market_id,
                "our_bid": rest_a.map(|o| o.price.clone()),
                "our_count": rest_a.map(|o| o.count),
                "reprices": rest_a.map(|o| o.reprices),
                "venue_bid": qa.and_then(|s| s.bid.clone()),
                "venue_ask": qa.and_then(|s| s.ask.clone()),
                "quote_age_s": q_age(qa),
            },
            "leg_b": {
                "venue": lb.venue, "market": lb.market_id,
                "our_ask": rest_b.map(|o| o.price.clone()),
                "our_count": rest_b.map(|o| o.count),
                "reprices": rest_b.map(|o| o.reprices),
                "venue_bid": qb.and_then(|s| s.bid.clone()),
                "venue_ask": qb.and_then(|s| s.ask.clone()),
                "quote_age_s": q_age(qb),
            },
            "priced": priced,
            "edge_f": best_edge,
            "rest_spread": rest_spread,
            "fillable": fillable,
            "take_depth": take_depth,
            "deep_enough": deep_enough,
            "have_books": have_books,
            // Time since this market's book last MOVED — information about how
            // quiet it is, deliberately not a staleness flag.
            "since_change_s": q_age(qa).unwrap_or(-1).max(q_age(qb).unwrap_or(-1)),
            "actionable": actionable,
            "edge": priced.as_ref().and_then(|p| p["edge_per_contract"].as_str().map(String::from)),
            "last_ts": rest_a.map(|o| o.ts).into_iter()
                        .chain(rest_b.map(|o| o.ts)).fold(0.0f64, f64::max),
        }));
    }

    // Actionable first. An un-fillable or stale-priced row is information, not
    // a trade, and must not head the list.
    rows.sort_by(|x, y| {
        let ok = |v: &serde_json::Value| v["actionable"].as_bool().unwrap_or(false);
        let f = |v: &serde_json::Value| {
            v["edge"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::MIN)
        };
        ok(y).cmp(&ok(x)).then(f(y).partial_cmp(&f(x)).unwrap_or(std::cmp::Ordering::Equal))
    });
    let actionable = rows.iter().filter(|v| v["actionable"].as_bool().unwrap_or(false)).count();

    serde_json::json!({
        "actionable": actionable,
        "max_rest_spread": 0.05,
        "min_take_depth": 25,
        "rollup_current": rollup_current,
        "rollup_coverage_age_s": if coverage_age_s == i64::MAX { -1 } else { coverage_age_s },
        "max_coverage_age_s": MAX_COVERAGE_AGE_S,
        "engine_live": age >= 0.0 && age < 120.0,
        "last_intent_age_s": age.round() as i64,
        "resting_orders": st.live.len(),
        "pairs": rows.len(),
        "opens": st.opens, "reprices": st.reprices, "cancels": st.cancels,
        "total": st.total,
        "clip": "25",
        "fee_category": FEE_CATEGORY,
        "maker_only": true,
        "rows": rows,
    })
    .to_string()
}

/// Post-fee edge over time for one pair.
///
/// Joins two independent series: our own posted quote (from the intent stream)
/// and the venue books (from the ToB rollup). Both forward-fill — a quote
/// stands until replaced — and at every point where enough is known we price
/// each available route. This is what makes "top N by spread" a time series
/// rather than a snapshot.
fn edge_series(a: &Args, rel: &arb_registry::Relationship) -> Vec<serde_json::Value> {
    let (la, lb) = (&rel.legs[0], &rel.legs[1]);
    let (Some(va), Some(vb)) = (venue_of(&la.venue), venue_of(&lb.venue)) else { return vec![] };

    let day = integrity::build(&a.data_dir).today;
    let path = |v: &str| format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir);
    let sa = series::load_market(&[path(&la.venue)], &la.venue, &la.market_id);
    let sb = series::load_market(&[path(&lb.venue)], &lb.venue, &lb.market_id);

    let text = std::fs::read_to_string(&a.intents_path).unwrap_or_default();
    let markets = vec![
        (la.venue.clone(), la.market_id.clone()),
        (lb.venue.clone(), lb.market_id.clone()),
    ];
    let ours = intents::history_for(&text, &markets);

    // One merged timeline in nanoseconds; intents carry seconds.
    let mut ts: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    ts.extend(sa.iter().map(|s| s.ts_local_ns));
    ts.extend(sb.iter().map(|s| s.ts_local_ns));
    ts.extend(ours.iter().map(|e| (e.ts * 1e9) as i64));

    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let clip = cx.parse_exact("25");

    let (mut ia, mut ib, mut io) = (0usize, 0usize, 0usize);
    let (mut ca, mut cb) = (None::<&arb_tob::TobSample>, None::<&arb_tob::TobSample>);
    let (mut our_a, mut our_b) = (None::<String>, None::<String>);
    let mut out = Vec::new();

    for t in ts {
        while ia < sa.len() && sa[ia].ts_local_ns <= t {
            ca = Some(&sa[ia]);
            ia += 1;
        }
        while ib < sb.len() && sb[ib].ts_local_ns <= t {
            cb = Some(&sb[ib]);
            ib += 1;
        }
        while io < ours.len() && (ours[io].ts * 1e9) as i64 <= t {
            let e = &ours[io];
            let on_a = e.venue == la.venue && e.market == la.market_id;
            // We rest a BID on leg A (buying YES) and an ASK on leg B.
            let v = if e.kind == "cancel" { None } else { Some(e.price.clone()) };
            if on_a && e.side == "bid" {
                our_a = v;
            } else if !on_a && e.side == "ask" {
                our_b = v;
            }
            io += 1;
        }

        let va_ask = ca.and_then(|s| s.ask.clone());
        let vb_bid = cb.and_then(|s| s.bid.clone());
        let mut row = serde_json::Map::new();
        let mut push = |label: &str, ra: Role, rb: Role, ea: &Option<String>, eb: &Option<String>,
                        cx: &mut Cx, row: &mut serde_json::Map<String, serde_json::Value>| {
            if let (Some(ea), Some(eb)) = (ea, eb) {
                if let Some(p) =
                    price_at(cx, &sched, "s", va, vb, ra, rb, ea, eb, clip, FEE_CATEGORY)
                {
                    if let Ok(v) = p.edge_per_contract.parse::<f64>() {
                        row.insert(label.into(), serde_json::json!(v));
                    }
                }
            }
        };
        push("take_take", Role::Taker, Role::Taker, &va_ask, &vb_bid, &mut cx, &mut row);
        push("make_a_take_b", Role::Maker, Role::Taker, &our_a, &vb_bid, &mut cx, &mut row);
        push("take_a_make_b", Role::Taker, Role::Maker, &va_ask, &our_b, &mut cx, &mut row);
        if row.is_empty() {
            continue;
        }
        row.insert("ts".into(), serde_json::json!(t as f64 / 1e9));
        out.push(serde_json::Value::Object(row));
    }
    out
}

/// Edge series for the top N pairs, so the intents page opens on a picture of
/// where the money has been rather than a single instant.
fn top_series_json(a: &Args, query: &str) -> String {
    let n: usize = query_param(query, "n").and_then(|s| s.parse().ok()).unwrap_or(5);
    let body = intents_json(a);
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) else {
        return "{\"error\":\"intents unavailable\"}".into();
    };
    let Ok(reg) = Registry::load(&a.registry) else {
        return "{\"error\":\"registry unavailable\"}".into();
    };
    let empty = vec![];
    let rows = v["rows"].as_array().unwrap_or(&empty);
    let mut out = Vec::new();
    for r in rows.iter().filter(|r| r["actionable"].as_bool().unwrap_or(false)).take(n) {
        let Some(id) = r["relationship_id"].as_str() else { continue };
        let Some(rel) = reg.get(id) else { continue };
        if rel.legs.len() != 2 {
            continue;
        }
        out.push(serde_json::json!({
            "relationship_id": id,
            "best_route": r["best_route"],
            "edge": r["edge"],
            "series": edge_series(a, rel),
        }));
    }
    serde_json::json!({ "n": out.len(), "rows": out }).to_string()
}

/// One pair's intent history — the engine's own quote, moving over time.
fn intent_series_json(a: &Args, query: &str) -> String {
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
    let text = std::fs::read_to_string(&a.intents_path).unwrap_or_default();
    let markets: Vec<(String, String)> =
        r.legs.iter().map(|l| (l.venue.clone(), l.market_id.clone())).collect();
    let events = intents::history_for(&text, &markets);
    serde_json::json!({
        "relationship_id": rel_id,
        "legs": markets.iter().map(|(v, m)| format!("{v}:{m}")).collect::<Vec<_>>(),
        "events": events,
        "edge_series": edge_series(a, r),
    })
    .to_string()
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
    // How far the rollup COVERS — the same gate the intents view applies, and
    // for the same reason. Ranking today's board off a rollup built this
    // morning is ranking this morning's board. Per-market sample age cannot
    // stand in for it: the series emits on change, so a quiet market's old
    // sample is still the current book.
    let coverage_ns = latest.values().map(|s| s.ts_local_ns).max().unwrap_or(0);
    let coverage_age_s =
        if coverage_ns > 0 { (now_ns - coverage_ns) / 1_000_000_000 } else { i64::MAX };
    const MAX_COVERAGE_AGE_S: i64 = 1800;
    let rollup_current = coverage_age_s <= MAX_COVERAGE_AGE_S;

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
        let qa = Quote {
            bid: sa.bid.clone(), bid_size: sa.bid_size.clone(),
            ask: sa.ask.clone(), ask_size: sa.ask_size.clone(),
        };
        let qb = Quote {
            bid: sb.bid.clone(), bid_size: sb.bid_size.clone(),
            ask: sb.ask.clone(), ask_size: sb.ask_size.clone(),
        };

        let mut by_scenario = serde_json::Map::new();
        let mut chosen: Option<arb_scenario::Priced> = None;
        for sc in Scenario::all() {
            if let Some(p) =
                price(&mut cx, &sched, sc, va, vb, &qa, &qb, clip, FEE_CATEGORY, max_spread)
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
            "intl_leg": la.venue == "polymarket" || lb.venue == "polymarket",
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
    // Actionable is a claim about NOW, so a rollup that stopped covering hours
    // ago cannot produce one. Without this the board happily reported "10
    // actionable" off 22-hour-old quotes.
    let actionable = rows
        .iter()
        .filter(|v| {
            rollup_current
                && v["priced"]["profitable"].as_bool().unwrap_or(false)
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
        "rollup_current": rollup_current,
        "rollup_coverage_age_s": if coverage_age_s == i64::MAX { -1 } else { coverage_age_s },
        "max_coverage_age_s": MAX_COVERAGE_AGE_S,
        "max_spread": max_spread_s,
        "fee_category": FEE_CATEGORY,
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
        "/" | "/recording" | "/opportunities" | "/pairs" | "/current" | "/intents"
        | "/trades" | "/live" => {
            respond(s, "200 OK", "text/html; charset=utf-8", PAGE)
        }
        // Long-lived: these return only when the client goes away.
        "/api/stream" => stream(s, a, sh),
        "/api/tape" => tape(s, a),
        "/api/books" => respond(s, "200 OK", "application/json", &books_json(a)),
        "/api/integrity" => {
            let i = integrity::build(&a.data_dir);
            let body = serde_json::to_string(&i).unwrap_or_else(|_| "{}".into());
            respond(s, "200 OK", "application/json", &body)
        }
        "/api/opportunities" => respond(s, "200 OK", "application/json", &opps_json(a, &query)),
        "/api/pairs" => respond(s, "200 OK", "application/json", &pairs_json(a)),
        "/api/intents" => respond(s, "200 OK", "application/json", &intents_json(a)),
        "/api/trades" => respond(s, "200 OK", "application/json", &trades_json(a)),
        "/api/top-series" => respond(s, "200 OK", "application/json", &top_series_json(a, &query)),
        "/api/intent-series" => {
            respond(s, "200 OK", "application/json", &intent_series_json(a, &query))
        }
        // The single write surface. GET reports status; only POST can start a
        // build, so nothing fires it by merely loading a page.
        "/api/rollup" => {
            if method == "POST" {
                respond(s, "200 OK", "application/json", &rollup_start(a, sh, &query))
            } else {
                respond(s, "200 OK", "application/json", &rollup_status(a, sh))
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
        ledger_path: "data/exec/trades.jsonl".into(),
        registry: "config/registry.yaml".into(),
        tradable: "config/tradable.yaml".into(),
        port: 4749,
        started_at: now_secs(),
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
            "--ledger" => a.ledger_path = v,
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
    // A thread per connection. Not for throughput — one person reads this —
    // but because /api/stream never returns, and a serial loop would let the
    // first subscriber wedge every later request.
    let args = Arc::new(a);
    for s in l.incoming().flatten() {
        let (a, sh) = (Arc::clone(&args), Arc::clone(&shared));
        std::thread::spawn(move || handle(s, &a, &sh));
    }
}
