//! arb-query — read layer over the recorded tape.
//!
//! The tape lives in two formats at once: today's day is append-only JSONL
//! (the recorder's write format — crash-safe and greppable) and closed days are
//! Parquet (the read format — columnar, ~20-25x smaller, and the reason a
//! one-relationship query is milliseconds instead of minutes). Callers should
//! never care which; `source_for` resolves it, exactly as
//! `src/arbbot/record/archive.py:60` does on the Python side.
//!
//! Deliberately NOT a query engine. It reads columns and folds them. Anything
//! that wants SQL over the whole archive is research, and research keeps
//! DuckDB.

use std::path::{Path, PathBuf};

pub mod opps;

/// Which physical file backs a `<stem>-<day>`, and in what format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Parquet(PathBuf),
    Jsonl(PathBuf),
}

impl Source {
    pub fn path(&self) -> &Path {
        match self {
            Source::Parquet(p) | Source::Jsonl(p) => p,
        }
    }
}

/// Parquet archive preferred, live JSONL as fallback, `None` if neither exists.
/// Mirrors `archive.source_for` so both stacks agree about which file is
/// authoritative for a day.
pub fn source_for(jsonl_dir: &str, parquet_dir: &str, stem: &str, day: &str) -> Option<Source> {
    let pq = PathBuf::from(parquet_dir).join(format!("{stem}-{day}.parquet"));
    if pq.is_file() {
        return Some(Source::Parquet(pq));
    }
    let jl = PathBuf::from(jsonl_dir).join(format!("{stem}-{day}.jsonl"));
    if jl.is_file() {
        return Some(Source::Jsonl(jl));
    }
    None
}

/// Every available source for `stem` across the inclusive day range, oldest
/// first. Missing days are skipped, not an error — a gap in the archive is a
/// fact to report, not a reason to fail a dashboard request.
pub fn sources_for_range(
    jsonl_dir: &str,
    parquet_dir: &str,
    stem: &str,
    from_day: &str,
    to_day: &str,
) -> Vec<Source> {
    let mut days: Vec<String> = Vec::new();
    for dir in [parquet_dir, jsonl_dir] {
        let Ok(rd) = std::fs::read_dir(dir) else { continue };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            let Some(base) = name.strip_suffix(".parquet").or_else(|| name.strip_suffix(".jsonl"))
            else {
                continue;
            };
            let Some(rest) = base.strip_prefix(stem) else { continue };
            let Some(day) = rest.strip_prefix('-') else { continue };
            if day.len() == 10
                && day.as_bytes()[4] == b'-'
                && day >= from_day
                && day <= to_day
                && !days.contains(&day.to_string())
            {
                days.push(day.to_string());
            }
        }
    }
    days.sort();
    days.iter()
        .filter_map(|d| source_for(jsonl_dir, parquet_dir, stem, d))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-test directory. Tests run in parallel, so keying only on pid lets one
    /// test's fixture files satisfy another's "should not exist" assertion.
    fn tmp(name: &str) -> PathBuf {
        let base =
            std::env::temp_dir().join(format!("arb-query-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::create_dir_all(base.join("scan"));
        let _ = std::fs::create_dir_all(base.join("parquet"));
        base
    }

    #[test]
    fn parquet_wins_over_jsonl_for_the_same_day() {
        let b = tmp("prefer");
        let (j, p) = (b.join("scan"), b.join("parquet"));
        let (js, ps) = (j.to_string_lossy().to_string(), p.to_string_lossy().to_string());
        std::fs::write(j.join("opportunities-2026-07-20.jsonl"), b"").unwrap();
        std::fs::write(p.join("opportunities-2026-07-20.parquet"), b"").unwrap();
        std::fs::write(j.join("opportunities-2026-07-21.jsonl"), b"").unwrap();

        // archived day -> parquet
        match source_for(&js, &ps, "opportunities", "2026-07-20") {
            Some(Source::Parquet(_)) => {}
            other => panic!("expected parquet, got {other:?}"),
        }
        // live day -> jsonl
        match source_for(&js, &ps, "opportunities", "2026-07-21") {
            Some(Source::Jsonl(_)) => {}
            other => panic!("expected jsonl, got {other:?}"),
        }
        assert!(source_for(&js, &ps, "opportunities", "2026-07-22").is_none());

        let all = sources_for_range(&js, &ps, "opportunities", "2026-07-19", "2026-07-30");
        assert_eq!(all.len(), 2, "{all:?}");
        // oldest first, and the archived day is not double-counted
        assert!(matches!(all[0], Source::Parquet(_)));
        assert!(matches!(all[1], Source::Jsonl(_)));

        let _ = std::fs::remove_dir_all(&b);
    }

    #[test]
    fn range_bounds_are_inclusive_and_filter() {
        let b = tmp("range");
        let (j, p) = (b.join("scan"), b.join("parquet"));
        let (js, ps) = (j.to_string_lossy().to_string(), p.to_string_lossy().to_string());
        for d in ["2026-07-20", "2026-07-21", "2026-07-22"] {
            std::fs::write(j.join(format!("opportunities-{d}.jsonl")), b"").unwrap();
        }
        let got = sources_for_range(&js, &ps, "opportunities", "2026-07-21", "2026-07-22");
        assert_eq!(got.len(), 2);
        assert!(got[0].path().to_string_lossy().contains("2026-07-21"));
        let _ = std::fs::remove_dir_all(&b);
    }
}
