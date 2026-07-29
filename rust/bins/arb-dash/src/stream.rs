//! The two push channels.
//!
//! `/api/stream` says WHICH view changed, cheaply enough to recompute once a
//! second forever. `/api/tape` says WHAT happened, line by line, as the engine
//! writes it. They are deliberately different shapes — see `tape` below.

use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, UNIX_EPOCH};

use crate::rollup::{self, Shared};
use crate::{integrity, Args, VENUES};

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
    let books = stat_all(&[
        format!("{}/kalshi_deposits.json", a.kalshi_dir),
        format!("{}/kalshi_fills.json", a.kalshi_dir),
        format!("{}/kalshi_settlements.json", a.kalshi_dir),
        format!("{}/pmus_balances.json", a.pmus_dir),
        format!("{}/pmus_positions.json", a.pmus_dir),
    ]);
    let recording = stat_all(
        &VENUES.iter().map(|v| format!("{}/raw/{v}-{day}.jsonl", a.data_dir)).collect::<Vec<_>>(),
    );
    let rollup = stat_all(
        &VENUES.iter().map(|v| format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir)).collect::<Vec<_>>(),
    );
    let intents = stat_sig(&a.intents_path);
    let opps = stat_sig(&format!("{}/opportunities-{day}.jsonl", a.scan_dir));
    let registry = stat_all(&[a.registry.clone(), a.tradable.clone()]);
    format!(
        "{{\"today\":\"{day}\",\"books\":{books},\"recording\":{recording},\
         \"rollup\":{rollup},\"intents\":{intents},\"opportunities\":{opps},\
         \"pairs\":{registry},\"rollup_status\":{}}}",
        rollup::status(a, sh)
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
pub fn state(mut s: TcpStream, a: &Args, sh: &Shared) {
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
pub fn tape(mut s: TcpStream, a: &Args) {
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
                let frame =
                    serde_json::json!({"src": label, "kind": classify(&parsed), "ev": parsed});
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

/// Classify here so the client can filter without parsing shapes. Skips
/// outnumber actions ~100:1 — the engine rejects far more than it places — so
/// a tape that cannot separate them shows only budget messages and hides every
/// real order.
fn classify(parsed: &serde_json::Value) -> &'static str {
    if parsed.get("skip").is_some() {
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
    }
}
