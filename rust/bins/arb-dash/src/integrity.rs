//! Recording integrity — the evidence that the tape is complete.
//!
//! Pure file-stat work, no tape parsing: it answers "is every day we recorded
//! either still raw or archived to Parquet, and is today's file actually
//! growing." That is exactly the check that was missing when `polymarket_us`
//! silently failed to archive for a week (31.7 GB of JSONL, zero Parquet)
//! because `arbbot-report.service` pipes the ETL to /dev/null.

use std::collections::{BTreeMap, BTreeSet};
use std::time::UNIX_EPOCH;

use arb_core::clock::now_secs;
use serde::Serialize;

const STEMS: [&str; 3] = ["kalshi", "polymarket", "polymarket_us"];

#[derive(Debug, Serialize)]
pub struct DayRow {
    pub stem: String,
    pub day: String,
    pub jsonl_bytes: Option<u64>,
    pub parquet_bytes: Option<u64>,
    /// A closed day with raw JSONL and no Parquet has failed to archive.
    pub unarchived: bool,
}

#[derive(Debug, Serialize)]
pub struct FeedRow {
    pub stem: String,
    pub path: String,
    pub bytes: u64,
    pub age_seconds: u64,
    pub stale: bool,
}

/// A file some other unit is supposed to keep fresh. Its age is the only
/// evidence the dash has that that unit is still alive.
#[derive(Debug, Serialize)]
pub struct DerivedRow {
    pub what: String,
    pub path: String,
    pub age_seconds: Option<u64>,
    pub max_age_seconds: u64,
    pub stale: bool,
}

#[derive(Debug, Serialize)]
pub struct Integrity {
    pub today: String,
    pub live_feeds: Vec<FeedRow>,
    pub derived: Vec<DerivedRow>,
    pub coverage: Vec<DayRow>,
    pub unarchived_days: usize,
    pub unarchived_bytes: u64,
    pub warnings: Vec<String>,
}

/// Files written by a timer, and how old each may be before the timer that
/// writes it must be assumed dead.
///
/// `marks.json` is here because on 2026-07-28 `arbbot-marks.service` had been
/// exiting non-zero every 2 minutes for over three hours
/// (`mark_positions.py:111`, `KeyError: 'cost_usd'` on the engine's
/// `fees_pending` record shape) while the engine kept re-deriving its take-take
/// APR bar from the frozen file and reporting the result with no age. Nothing
/// on this dashboard or in the engine said a word. The engine-side staleness
/// guard belongs in the engine; this row is so an operator can SEE it.
const DERIVED: [(&str, &str, u64); 1] = [(
    "position marks (arbbot-marks.timer)",
    "exec/marks.json",
    600,
)];

fn scan_dir(dir: &str, suffix: &str) -> BTreeMap<(String, String), u64> {
    let mut out = BTreeMap::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(_) => return out,
    };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let Some(base) = name.strip_suffix(suffix) else { continue };
        // "<stem>-<YYYY-MM-DD>"
        let Some(idx) = base.rfind('-') else { continue };
        let Some(idx) = base[..idx].rfind('-') else { continue };
        let Some(idx) = base[..idx].rfind('-') else { continue };
        let (stem, day) = base.split_at(idx);
        let day = day.trim_start_matches('-').to_string();
        if !STEMS.contains(&stem) {
            continue;
        }
        let size = e.metadata().map(|m| m.len()).unwrap_or(0);
        out.insert((stem.to_string(), day), size);
    }
    out
}

pub fn build(data_dir: &str) -> Integrity {
    let now = now_secs();
    let today = arb_core::resolve::iso_from_day(now as i64 / 86_400);
    let raw = scan_dir(&format!("{data_dir}/raw"), ".jsonl");
    let pq = scan_dir(&format!("{data_dir}/parquet"), ".parquet");

    let mut days: BTreeSet<(String, String)> = BTreeSet::new();
    days.extend(raw.keys().cloned());
    days.extend(pq.keys().cloned());

    let mut coverage = Vec::new();
    let mut unarchived_days = 0usize;
    let mut unarchived_bytes = 0u64;
    for (stem, day) in days {
        let j = raw.get(&(stem.clone(), day.clone())).copied();
        let p = pq.get(&(stem.clone(), day.clone())).copied();
        // Today is legitimately raw-only; earlier days are not.
        let unarchived = day < today && j.is_some() && p.is_none();
        if unarchived {
            unarchived_days += 1;
            unarchived_bytes += j.unwrap_or(0);
        }
        coverage.push(DayRow {
            stem: stem.clone(),
            day: day.clone(),
            jsonl_bytes: j,
            parquet_bytes: p,
            unarchived,
        });
    }

    let mut live_feeds = Vec::new();
    let mut warnings = Vec::new();
    for stem in STEMS {
        let path = format!("{data_dir}/raw/{stem}-{today}.jsonl");
        match std::fs::metadata(&path) {
            Ok(m) => {
                let mtime = m
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let age = now.saturating_sub(mtime);
                // The recorder's own liveness gate is 10s per connection; 120s
                // of no bytes at all on a venue's tape is unambiguous.
                let stale = age > 120;
                if stale {
                    warnings.push(format!("{stem} tape has not grown in {age}s"));
                }
                live_feeds.push(FeedRow {
                    stem: stem.into(),
                    path,
                    bytes: m.len(),
                    age_seconds: age,
                    stale,
                });
            }
            Err(_) => {
                warnings.push(format!("{stem}: no tape for {today}"));
            }
        }
    }
    if unarchived_days > 0 {
        warnings.push(format!(
            "{unarchived_days} closed day(s) never archived to Parquet ({:.1} GB still raw)",
            unarchived_bytes as f64 / 1e9
        ));
    }

    let mut derived = Vec::new();
    for (what, rel, max_age) in DERIVED {
        let path = format!("{data_dir}/{rel}");
        let age = std::fs::metadata(&path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| now.saturating_sub(d.as_secs()));
        // An absent file is stale too — a writer that has never run is not
        // better news than one that stopped.
        let stale = age.map(|a| a > max_age).unwrap_or(true);
        if stale {
            warnings.push(match age {
                Some(a) => format!("{what} is {a}s old (limit {max_age}s) — its writer has died"),
                None => format!("{what}: {path} does not exist"),
            });
        }
        derived.push(DerivedRow {
            what: what.into(),
            path,
            age_seconds: age,
            max_age_seconds: max_age,
            stale,
        });
    }

    Integrity { today, live_feeds, derived, coverage, unarchived_days, unarchived_bytes, warnings }
}
