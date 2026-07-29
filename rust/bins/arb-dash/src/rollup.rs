//! The rollup trigger — the dashboard's only write surface.
//!
//! Everything else here reads. This starts `arb_tob::build_day` on a worker
//! thread, and the state below exists only to keep two triggers from racing.

use std::sync::{Arc, Mutex};

use arb_tob::{build_day, DEFAULT_INTERVAL_NS};

use crate::endpoints::age_secs;
use crate::http::query_param;
use crate::{integrity, Args, VENUES};

/// The one piece of mutable state in the process, and it exists only so a
/// second trigger cannot start while a build is in flight.
#[derive(Default)]
pub struct Rollup {
    running_day: Option<String>,
    last: Option<serde_json::Value>,
}

pub type Shared = Arc<Mutex<Rollup>>;

/// Status of the rollup, including the part that outlives this process. The
/// old version reported only builds from THIS session, so a fresh restart said
/// "no build this session" over a series that had been on disk since noon —
/// and a completed build showed its duration but never when it finished. The
/// series is written atomically at the end of a build, so the file's mtime IS
/// the build's completion time.
pub fn status(a: &Args, sh: &Shared) -> String {
    let g = sh.lock().unwrap_or_else(|e| e.into_inner());
    let day = integrity::build(&a.data_dir).today;
    let built_age_s = VENUES
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
pub fn start(a: &Args, sh: &Shared, query: &str) -> String {
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
        let res = build_day(&raw, &pq, &out, &d2, DEFAULT_INTERVAL_NS, &VENUES);
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
