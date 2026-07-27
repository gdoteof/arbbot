//! Opportunity tape reader + per-relationship rollup.
//!
//! Only the columns the views need are projected. In particular the nested
//! `basket` LIST<STRUCT> is skipped: it is the widest column in the file and no
//! aggregate view reads it, so projecting it away is both a correctness
//! simplification and the bulk of the speedup.
//!
//! Edges are parsed as f64 HERE and only here. That is safe because these are
//! reporting aggregates (max/count over observations), never money that gets
//! posted. Anything that lands in the books goes through `arb-ledger` with
//! exact decimals — see the sign convention note in that crate.

use std::collections::HashMap;
use std::fs::File;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;
use serde::Serialize;

use crate::Source;

/// Physical schema subset. Must match the writer's types exactly or the
/// projection is rejected; see `parquet_schema()` on the archived file.
const PROJECTION: &str = "
message duckdb_schema {
  OPTIONAL BYTE_ARRAY relationship_id (UTF8);
  OPTIONAL BYTE_ARRAY net_edge_per_contract (UTF8);
  OPTIONAL BYTE_ARRAY size (UTF8);
  OPTIONAL INT64 ts_local_ns (INT_64);
}
";

const NS_PER_MINUTE: i64 = 60_000_000_000;

#[derive(Debug, Clone, Serialize)]
pub struct RelSummary {
    pub relationship_id: String,
    /// Logged observations. NOT episodes — the scan log only records ticks that
    /// already cleared the edge bar, so this counts a standing dislocation many
    /// times over. Episode counting belongs to a separate pass.
    pub observations: u64,
    pub minutes_seen: u64,
    pub best_edge_per_contract: f64,
    pub best_size: f64,
    pub first_ts_ns: i64,
    pub last_ts_ns: i64,
}

#[derive(Debug, Default)]
struct Acc {
    observations: u64,
    best_edge: f64,
    best_size: f64,
    first_ts: i64,
    last_ts: i64,
    minutes: std::collections::HashSet<i64>,
}

/// Fold one or more days into a per-relationship summary. `only` filters to a
/// single relationship when set — the drill-down path.
///
/// Days are read in PARALLEL, one thread per file. This is the cheapest large
/// win available: the tape is time-ordered, not relationship-ordered, so a
/// row-group min/max on `relationship_id` spans nearly every value and prunes
/// almost nothing — predicate pushdown does not help here the way it would on
/// clustered data. Measured 2026-07-27, 7 archived days / 6.16M rows:
/// 2.26 s serial. DuckDB does the same fold in 0.088 s; we are not trying to
/// match it, only to stay under a comfortable page-load budget.
pub fn summarize(sources: &[Source], only: Option<&str>) -> Result<Vec<RelSummary>, String> {
    let mut acc: HashMap<String, Acc> = HashMap::new();
    let parts: Vec<Result<HashMap<String, Acc>, String>> = std::thread::scope(|s| {
        let handles: Vec<_> = sources
            .iter()
            .map(|src| s.spawn(move || summarize_one(src, only)))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err("reader panicked".into()))).collect()
    });
    for part in parts {
        merge(&mut acc, part?);
    }

    let mut out: Vec<RelSummary> = acc
        .into_iter()
        .map(|(relationship_id, a)| RelSummary {
            relationship_id,
            observations: a.observations,
            minutes_seen: a.minutes.len() as u64,
            best_edge_per_contract: a.best_edge,
            best_size: a.best_size,
            first_ts_ns: a.first_ts,
            last_ts_ns: a.last_ts,
        })
        .collect();
    out.sort_by(|x, y| {
        y.best_edge_per_contract
            .partial_cmp(&x.best_edge_per_contract)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(out)
}

fn merge(into: &mut HashMap<String, Acc>, from: HashMap<String, Acc>) {
    for (rel, a) in from {
        match into.get_mut(&rel) {
            None => {
                into.insert(rel, a);
            }
            Some(e) => {
                e.observations += a.observations;
                if a.best_edge > e.best_edge {
                    e.best_edge = a.best_edge;
                }
                if a.best_size > e.best_size {
                    e.best_size = a.best_size;
                }
                if a.first_ts < e.first_ts {
                    e.first_ts = a.first_ts;
                }
                if a.last_ts > e.last_ts {
                    e.last_ts = a.last_ts;
                }
                e.minutes.extend(a.minutes);
            }
        }
    }
}

fn summarize_one(src: &Source, only: Option<&str>) -> Result<HashMap<String, Acc>, String> {
    let proj = parse_message_type(PROJECTION).map_err(|e| format!("projection: {e}"))?;
    let mut acc: HashMap<String, Acc> = HashMap::new();
    {
        match src {
            Source::Parquet(p) => {
                let f = File::open(p).map_err(|e| format!("open {}: {e}", p.display()))?;
                let reader =
                    SerializedFileReader::new(f).map_err(|e| format!("{}: {e}", p.display()))?;
                let iter = reader
                    .get_row_iter(Some(proj.clone()))
                    .map_err(|e| format!("{}: {e}", p.display()))?;
                for row in iter {
                    let row = row.map_err(|e| format!("{}: {e}", p.display()))?;
                    let rel = row.get_string(0).map(|s| s.as_str()).unwrap_or("");
                    if only.is_some_and(|o| o != rel) {
                        continue;
                    }
                    let edge = row.get_string(1).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let size = row.get_string(2).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ts = row.get_long(3).unwrap_or(0);
                    ingest(&mut acc, rel, edge, size, ts);
                }
            }
            Source::Jsonl(p) => {
                // Today's live file — one file, so cross-file parallelism does
                // not help and this becomes the bottleneck (measured 2.1 s on a
                // 700 MB tape). Split it into byte chunks aligned to newlines
                // and parse them concurrently instead.
                let text = std::fs::read_to_string(p)
                    .map_err(|e| format!("read {}: {e}", p.display()))?;
                let n = std::thread::available_parallelism()
                    .map(|v| v.get())
                    .unwrap_or(4)
                    .min(16)
                    .max(1);
                let bounds = chunk_bounds(&text, n);
                let parts: Vec<HashMap<String, Acc>> = std::thread::scope(|s| {
                    let hs: Vec<_> = bounds
                        .windows(2)
                        .map(|w| {
                            let slice = &text[w[0]..w[1]];
                            s.spawn(move || fold_jsonl(slice, only))
                        })
                        .collect();
                    hs.into_iter().map(|h| h.join().unwrap_or_default()).collect()
                });
                for part in parts {
                    merge(&mut acc, part);
                }
            }
        }
    }
    Ok(acc)
}

/// Byte offsets splitting `text` into ~`n` pieces, each starting just after a
/// newline so no JSON object is cut in half. Always returns at least [0, len].
fn chunk_bounds(text: &str, n: usize) -> Vec<usize> {
    let len = text.len();
    let mut bounds = vec![0usize];
    if n > 1 && len > 0 {
        let step = len / n;
        let bytes = text.as_bytes();
        for i in 1..n {
            let mut at = (step * i).min(len);
            while at < len && bytes[at] != b'\n' {
                at += 1;
            }
            if at < len {
                at += 1; // start after the newline
            }
            if at > *bounds.last().unwrap() && at < len {
                bounds.push(at);
            }
        }
    }
    bounds.push(len);
    bounds
}

/// Fold one JSONL slice. Tolerates a torn final line and NUL runs from an
/// unclean shutdown (observed 2026-07-27: 2,795 NUL bytes in the scan tape
/// after the 08:21 crash) — a damaged line is skipped, not fatal.
fn fold_jsonl(slice: &str, only: Option<&str>) -> HashMap<String, Acc> {
    let mut acc: HashMap<String, Acc> = HashMap::new();
    for line in slice.lines() {
        let t = line.trim_matches(char::from(0)).trim();
        if t.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(t) else { continue };
        let rel = v.get("relationship_id").and_then(|x| x.as_str()).unwrap_or("");
        if rel.is_empty() || only.is_some_and(|o| o != rel) {
            continue;
        }
        let edge = v
            .get("net_edge_per_contract")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let size = v
            .get("size")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let ts = v.get("ts_local_ns").and_then(|x| x.as_i64()).unwrap_or(0);
        ingest(&mut acc, rel, edge, size, ts);
    }
    acc
}

fn ingest(acc: &mut HashMap<String, Acc>, rel: &str, edge: f64, size: f64, ts: i64) {
    let e = acc.entry(rel.to_string()).or_default();
    if e.observations == 0 {
        e.best_edge = f64::MIN;
        e.first_ts = ts;
    }
    e.observations += 1;
    if edge > e.best_edge {
        e.best_edge = edge;
    }
    if size > e.best_size {
        e.best_size = size;
    }
    if ts < e.first_ts {
        e.first_ts = ts;
    }
    if ts > e.last_ts {
        e.last_ts = ts;
    }
    e.minutes.insert(ts / NS_PER_MINUTE);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn write_jsonl(lines: &[&str]) -> PathBuf {
        let p = std::env::temp_dir()
            .join(format!("arb-query-opps-{}-{}.jsonl", std::process::id(), lines.len()));
        std::fs::write(&p, lines.join("\n")).unwrap();
        p
    }

    #[test]
    fn folds_jsonl_and_survives_nul_damage() {
        let p = write_jsonl(&[
            r#"{"relationship_id":"a","net_edge_per_contract":"0.05","size":"10","ts_local_ns":60000000000}"#,
            "\0\0\0\0",
            r#"{"relationship_id":"a","net_edge_per_contract":"0.09","size":"4","ts_local_ns":120000000000}"#,
            r#"{"relationship_id":"b","net_edge_per_contract":"0.01","size":"2","ts_local_ns":60000000000}"#,
            r#"{"truncated":"#,
        ]);
        let got = summarize(&[Source::Jsonl(p.clone())], None).expect("folds");
        assert_eq!(got.len(), 2);
        // sorted by best edge desc
        assert_eq!(got[0].relationship_id, "a");
        assert_eq!(got[0].observations, 2);
        assert_eq!(got[0].minutes_seen, 2);
        assert!((got[0].best_edge_per_contract - 0.09).abs() < 1e-12);
        assert!((got[0].best_size - 10.0).abs() < 1e-12);
        assert_eq!(got[1].relationship_id, "b");
        let _ = std::fs::remove_file(p);
    }

    /// Chunking must never split a JSON object, and must produce the same
    /// answer as a single-threaded fold regardless of chunk count.
    #[test]
    fn chunked_parse_matches_serial_and_never_splits_a_record() {
        let mut lines = Vec::new();
        for i in 0..500 {
            lines.push(format!(
                r#"{{"relationship_id":"r{}","net_edge_per_contract":"0.0{}","size":"1","ts_local_ns":{}}}"#,
                i % 7, i % 9, i as i64 * 1_000_000_000
            ));
        }
        let text = lines.join("\n");
        let serial = fold_jsonl(&text, None);
        for n in [1usize, 2, 3, 7, 16, 64] {
            let bounds = chunk_bounds(&text, n);
            let mut got: HashMap<String, Acc> = HashMap::new();
            for w in bounds.windows(2) {
                merge(&mut got, fold_jsonl(&text[w[0]..w[1]], None));
            }
            assert_eq!(got.len(), serial.len(), "n={n} rel count");
            let tot: u64 = got.values().map(|a| a.observations).sum();
            let want: u64 = serial.values().map(|a| a.observations).sum();
            assert_eq!(tot, want, "n={n} observations (a split record would lose one)");
            assert_eq!(tot, 500, "n={n}");
        }
    }

    #[test]
    fn only_filter_restricts_to_one_relationship() {
        let p = write_jsonl(&[
            r#"{"relationship_id":"a","net_edge_per_contract":"0.05","size":"1","ts_local_ns":0}"#,
            r#"{"relationship_id":"b","net_edge_per_contract":"0.50","size":"1","ts_local_ns":0}"#,
        ]);
        let got = summarize(&[Source::Jsonl(p.clone())], Some("a")).expect("folds");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].relationship_id, "a");
        let _ = std::fs::remove_file(p);
    }
}
